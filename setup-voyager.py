import os
import json
import time
import fire
import subprocess
import oyaml as yaml
from kubernetes import client, config


def run(cmd, shell=True, capture_output=True, text=True, halt_exception=True):
    try:
        out = subprocess.run(cmd, shell=shell, capture_output=capture_output, text=text)
        # print(cmd)
        if out.stderr:
            if halt_exception:
                raise Exception("Command Error:" + str(out.stderr))
            else:
                print("Command Error:" + str(out.stderr))
        if capture_output:
            return out.stdout.rstrip("\n")
        else:
            return None
    except Exception as e:
        print("Command Unsuccessful:", cmd)
        print(str(e))
        raise Exception


def wait_till_delete(namespace, label_selector, v1):
    def pod_exists():
        try:
            pods = v1.list_namespaced_pod(namespace, label_selector=label_selector)
            return len(pods.items) > 0
        except Exception as _:
            return False
    while pod_exists():
        time.sleep(2)


class CerebroInstaller:
    def __init__(self):
        self.username = None
        self.namespace = None
        self.num_workers = None
        self.values_yaml = None

        # read values YAML file
        with open('values.yaml', 'r') as yaml_file:
            self.values_yaml = yaml.safe_load(yaml_file)

        # get username and update in values YAML file
        if "<username>" in self.values_yaml["cluster"]["username"]:
            username = run("whoami")
            self.values_yaml["cluster"]["username"] = self.values_yaml["cluster"]["username"].replace("<username>", username)
            with open("values.yaml", "w") as f:
                yaml.safe_dump(self.values_yaml, f)

        # set commonly used values
        self.username = run("whoami")
        self.namespace = self.values_yaml["cluster"]["namespace"]
        self.num_workers = self.values_yaml["cluster"]["numWorkers"]

    def install_kvs(self):
        # schedule KVS with username prefixed, on any Goya compute node
        cmds = [
            "helm repo add bitnami https://charts.bitnami.com/bitnami",
            "helm repo update",
            "helm install {}-redis bitnami/redis \
                --namespace {} \
                --set architecture=standalone \
                --set global.redis.password=cerebro \
                --set disableCommands[0]= \
                --set disableCommands[1]= \
                --set master.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key=brightcomputing.com/node-category \
                --set master.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator=In \
                --set master.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].values[0]=goya \
                --wait".format(self.username, self.namespace)
        ]

        for cmd in cmds:
            run(cmd)

        print("Installed Redis DB successfully")

    def init_cerebro(self):
        config.load_kube_config()
        v1 = client.CoreV1Api()

        # add hardware info configmap
        node_hardware_info = {}
        for i in range(self.num_workers):
            node_hardware_info["node" + str(i)] = {
                "num_cores": self.values_yaml["cluster"]["resourceRequests"]["workerCPU"],
                "num_gpus": self.values_yaml["cluster"]["resourceRequests"]["workerGPU"]
            }

        # create node hardware info configmap
        configmap = client.V1ConfigMap(data={"data": json.dumps(node_hardware_info)}, metadata=client.V1ObjectMeta(name="node-hardware-info"))
        v1.create_namespaced_config_map(namespace=self.namespace, body=configmap)
        print("Created configmap for node hardware info")

        # make configmap of select values.yaml values
        configmap_values = {
            "username": self.username,
            "namespace": self.values_yaml["cluster"]["namespace"],
            "controller_data_path": self.values_yaml["controller"]["volumes"]["dataPath"],
            "worker_rpc_port": self.values_yaml["worker"]["rpcPort"],
            "user_repo_path": self.values_yaml["controller"]["volumes"]["userRepoPath"],
            "server_backend_port": self.values_yaml["server"]["backendPort"],
            "jupyter_token_string": self.values_yaml["creds"]["jupyterTokenSting"],
            "jupyter_node_port": self.values_yaml["controller"]["services"]["jupyterNodePort"],
            "tensorboard_node_port": self.values_yaml["controller"]["services"]["tensorboardNodePort"],
            "grafana_node_port": self.values_yaml["cluster"]["networking"]["grafanaNodePort"],
            "prometheus_node_port": self.values_yaml["cluster"]["networking"]["prometheusNodePort"],
            "loki_port": self.values_yaml["cluster"]["networking"]["lokiPort"],
            "shard_multiplicity": self.values_yaml["worker"]["shardMultiplicity"],
            "sample_size": self.values_yaml["worker"]["sampleSize"],
        }

        # create cerebro info configmap
        configmap = client.V1ConfigMap(data={"data": json.dumps(configmap_values)}, metadata=client.V1ObjectMeta(name="cerebro-info"))
        v1.create_namespaced_config_map(namespace=self.namespace, body=configmap)
        print("Created configmap for Cerebro values info")

    def create_controller(self):
        cmds = [
            "mkdir -p charts",
            "helm create charts/cerebro-controller",
            "rm -rf charts/cerebro-controller/templates/*",
            "cp ./controller/* charts/cerebro-controller/templates/",
            "cp values.yaml charts/cerebro-controller/values.yaml",
            "helm install --namespace={} {}-cerebro-controller charts/cerebro-controller/".format(self.namespace, self.username),
            "rm -rf charts"
        ]

        for cmd in cmds:
            time.sleep(0.5)
            run(cmd, capture_output=False)
        print("Created Controller deployment")

        config.load_kube_config()
        v1 = client.AppsV1Api()
        ready = False
        deployment_name = "{}-cerebro-controller".format(self.username)

        while not ready:
            rollout = v1.read_namespaced_deployment_status(name=deployment_name, namespace=self.namespace)
            if rollout.status.ready_replicas == rollout.status.replicas:
                print("Controller created successfully")
                ready = True
            else:
                time.sleep(1)

    def create_workers(self):
        # create ETL Workers
        cmds = [
            "mkdir -p charts",
            "helm create charts/cerebro-etl-worker",
            "rm -rf charts/cerebro-etl-worker/templates/*",
            "cp etl-worker/* charts/cerebro-etl-worker/templates/",
            "cp values.yaml charts/cerebro-etl-worker/values.yaml",
            "helm install --namespace={} {}-etl-worker charts/cerebro-etl-worker".format(self.namespace, self.username),
            "rm -rf charts"
        ]

        for cmd in cmds:
            time.sleep(0.5)
            run(cmd, capture_output=False)

        print("ETL Workers created successfully")

        # create MOP Workers
        cmds = [
            "mkdir -p charts",
            "helm create charts/cerebro-mop-worker",
            "rm -rf charts/cerebro-mop-worker/templates/*",
            "cp mop-worker/* charts/cerebro-mop-worker/templates/",
            "cp values.yaml charts/cerebro-mop-worker/values.yaml",
            "helm install --namespace={} {}-mop-worker charts/cerebro-mop-worker".format(self.namespace, self.username),
            "rm -rf charts"
        ]

        for cmd in cmds:
            time.sleep(0.5)
            run(cmd, capture_output=False)

        print("MOP Workers created successfully")

    def create_server(self):
        cmds = [
            "mkdir -p charts",
            "helm create charts/cerebro-server",
            "rm -rf charts/cerebro-server/templates/*",
            "cp ./server/* charts/cerebro-server/templates/",
            "cp values.yaml charts/cerebro-server/values.yaml",
            "helm install --namespace={} {}webapp charts/cerebro-server/".format(self.namespace, self.username),
            "rm -rf charts"
        ]

        for cmd in cmds:
            time.sleep(0.5)
            run(cmd, capture_output=False)

        print("Created Server Deployment")

        config.load_kube_config()
        v1 = client.AppsV1Api()
        ready = False
        deployment_name = "{}-cerebro-server".format(self.username)

        while not ready:
            rollout = v1.read_namespaced_deployment_status(name=deployment_name, namespace=self.namespace)
            if rollout.status.ready_replicas == rollout.status.replicas:
                print("Server created successfully")
                ready = True
            else:
                time.sleep(1)

    def shutdown_cerebro(self):
        # load kubernetes config
        config.load_kube_config()
        v1 = client.CoreV1Api()

        # clean up Workers
        try:
            cmd1 = "helm delete {}-etl-worker -n {}".format(self.username, self.namespace)
            run(cmd1, capture_output=False)
            cmd2 = "helm delete {}-mop-worker -n {}".format(self.username, self.namespace)
            run(cmd2, capture_output=False)

            etl_label_selector = "app=cerebro-etl-worker,user={}".format(self.username)
            mop_label_selector = "app=cerebro-mop-worker,user={}".format(self.username)
            wait_till_delete(self.namespace, etl_label_selector, v1)
            wait_till_delete(self.namespace, mop_label_selector, v1)

        except Exception as _:
            print("Got an error while cleaning up Workers")

        print("Cleaned up Workers")

        # clean up Key-Value-Store (not checking if deleted!)
        try:
            cmd3 = "helm delete {}-redis -n {}".format(self.username, self.namespace)
            run(cmd3, capture_output=False, halt_exception=False)
            time.sleep(5)
        except Exception as _:
            print("Got an error while cleaning up Key Value Store")
        print("Cleaned up Key Value Store")

        # clean up Controller
        try:
            cmd4 = "helm delete {}-controller -n {}".format(self.username, self.namespace)
            run(cmd4, halt_exception=False)
            label_selector = "app=cerebro-controller,user={}".format(self.username)
            wait_till_delete(self.namespace, label_selector, v1)
        except Exception as e:
            print("Got error while cleaning up Controller: " + str(e))
        print("Cleaned up Controller")

        # clean up server
        try:
            cmd5 = "helm delete {}-server -n {}".format(self.username, self.namespace)
            run(cmd5, halt_exception=False)
            label_selector = "app=cerebro-server,user={}".format(self.username)
            wait_till_delete(self.namespace, label_selector, v1)
            print("Cleaned up Server")
        except Exception as e:
            print("Got error while cleaning up Server: " + str(e))

        # clear out hostPath Volumes
        root_host_path = self.values_yaml["controller"]["volumes"]["baseHostPath"].replace("*username", self.username)
        try:
            os.rmdir(root_host_path)
            print("Root Volumes successfully deleted.")
        except OSError as e:
            print(f"Error: {e}")

        print("Uninstalled Cerebro!")

    def testing(self):
        pass

    def install_cerebro(self):
        # install Key-Value Store
        self.install_kvs()

        # initialize basic cerebro components
        self.init_cerebro()

        # creates Controller
        self.create_controller()

        # create webapp
        self.create_server()

        # create Workers
        self.create_workers()

        # url = self.values_yaml["cluster"]["networking"]["publicDNSName"]
        # port = self.values_yaml["webApp"]["uiNodePort"]
        # time.sleep(5)
        # print("You can access the cluster using this URL:")
        # print("http://{}:{}".format(url, port))


if __name__ == '__main__':
    fire.Fire(CerebroInstaller)
