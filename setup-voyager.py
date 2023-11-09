import os
import json
import time
import fire
import subprocess
import oyaml as yaml
from kubernetes import client, config

# TODO:
# • delete all hostPath directories in cleanUp - how to do this? postStop

def run(cmd, shell=True, capture_output=True, text=True, haltException=True):
    try:
        out = subprocess.run(cmd, shell=shell, capture_output=capture_output, text=text)
        # print(cmd)
        if out.stderr:
            if haltException:
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

class CerebroInstaller:
    def __init__(self):
        self.username = None
        self.num_workers = None
        self.values_yaml = None

        # read values YAML file
        with open('values.yaml', 'r') as yamlfile:
            self.values_yaml = yaml.safe_load(yamlfile)
            
        # get username and update in values YAML file
        if not self.values_yaml["user"]["username"]:
            username = run("whoami")
            self.values_yaml["user"]["username"] = username
            with open("values.yaml", "w") as f:
                yaml.safe_dump(self.values_yaml, f)
        
        # set commonly used values        
        self.username = self.values_yaml["user"]["username"]
        self.num_workers = self.values_yaml["cluster"]["numWorkers"]

    def installKeyValueStore(self):
        # schedule KVS with username prefixed, on any Goya compute node
        
        username = self.username
        cmds = [
            "helm repo add bitnami https://charts.bitnami.com/bitnami",
            "helm repo update",
            "helm install {}-redis bitnami/redis \
                --namespace default \
                --set master.nodeSelector.brightcomputing.com/node-category=goya \
                --set architecture=standalone \
                --set global.redis.password=cerebro \
                --set disableCommands[0]= \
                --set disableCommands[1]= \
                --wait".format(username)
        ]

        for cmd in cmds:
            run(cmd)

        print("Installed Redis DB successfully")

    def initCerebro(self):
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
        v1.create_namespaced_config_map(namespace=self.kube_namespace, body=configmap)
        print("Created configmap for node hardware info")

        # make configmap of select values.yaml values
        configmap_values = {
            "username": self.values_yaml["cluster"]["username"],
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
        v1.create_namespaced_config_map(namespace=self.kube_namespace, body=configmap)
        print("Created configmap for Cerebro values info")

    def createController(self):
        cmds = [
            "mkdir -p charts",
            "helm create charts/cerebro-controller",
            "rm -rf charts/cerebro-controller/templates/*",
            "cp ./controller/* charts/cerebro-controller/templates/",
            "cp values.yaml charts/cerebro-controller/values.yaml",
            "helm install --namespace=cerebro {}-cerebro-controller charts/cerebro-controller/".format(self.username),
            "rm -rf charts/cerebro-controller"
        ]

        for cmd in cmds:
            time.sleep(1)
            run(cmd, capture_output=False)

        print("Created Controller deployment")

        config.load_kube_config()
        v1 = client.AppsV1Api()
        ready = False
        deployment = v1.list_namespaced_deployment(namespace="cerebro", label_selector="app=cerebro-controller").items[0]
        deployment_name = deployment.metadata.name

        while not ready:
            rollout = v1.read_namespaced_deployment_status(name=deployment_name, namespace=self.kube_namespace)
            if rollout.status.ready_replicas == rollout.status.replicas:
                print("Controller ready")
                ready = True
            else:
                time.sleep(1)

    def createWorkers(self):
        # load fabric connections
        self.initializeFabric()

        # create Workers
        cmds = [
            "mkdir -p charts",
            "helm create charts/cerebro-worker",
            "rm -rf charts/cerebro-worker/templates/*",
            "cp worker/* charts/cerebro-worker/templates/",
            "cp values.yaml charts/cerebro-worker/values.yaml",
            "helm install --namespace={} worker charts/cerebro-worker".format(self.kube_namespace)
        ]

        for cmd in cmds:
            time.sleep(0.5)
            run(cmd, capture_output=False)
        run("rm -rf charts")

        print("Waiting for ETL Worker start-up")
        time.sleep(5)

        print("Workers created successfully")

    def createWebApp(self):
        # load fabric connections
        self.initializeFabric()

        cmds = [
            "mkdir -p charts",
            "helm create charts/cerebro-webapp",
            "rm -rf charts/cerebro-webapp/templates/*",
            "cp ./webapp/* charts/cerebro-webapp/templates/",
            "cp values.yaml charts/cerebro-webapp/values.yaml",
            "helm install --namespace=cerebro webapp charts/cerebro-webapp/",
            "rm -rf charts/cerebro-webapp"
        ]

        for cmd in cmds:
            time.sleep(1)
            _ = run(cmd, capture_output=False)

        print("Created WebApp deployment")

        config.load_kube_config()
        v1 = client.AppsV1Api()
        ready = False
        deployment = v1.list_namespaced_deployment(namespace=self.kube_namespace, label_selector='app=cerebro-webapp').items[0]
        deployment_name = deployment.metadata.name

        while not ready:
            rollout = v1.read_namespaced_deployment_status(name=deployment_name, namespace=self.kube_namespace)
            if rollout.status.ready_replicas == rollout.status.replicas:
                print("Webapp ready")
                ready = True
            else:
                time.sleep(1)

    def cleanUp(self):
        # TODO: reset all data from prometheus

        # load fabric connections
        self.initializeFabric()

        # load kubernetes config
        config.load_kube_config()
        v1 = client.CoreV1Api()

        # clean up Workers
        try:
            cmd2 = "helm delete worker"
            run(cmd2, capture_output=False, haltException=False)

            for i in range(self.num_workers):
                v1.delete_namespaced_persistent_volume_claim(name="cerebro-data-storage-worker-cerebro-worker-{}".format(i), namespace=self.kube_namespace)
                print("Deleted PVC cerebro-data-storage-worker-cerebro-worker-{}".format(i))
        except Exception as e:
            print("Got error while cleaning up Workers: " + str(e))

        print("Cleaned up Workers")

        # wipe Key-Value Store
        keys = ["heartbeat", "kvs_init", "etl_task", "etl_func", "etl_worker_status", "etl_worker_progress"]
        keys2 = ["mop_spec", "mop_task", "mop_model_on_worker", "mop_model_parallelism_on_worker", "mop_worker_status", "mop_model_mapping", "mop_parallelism_mapping", "mop_sample_time"]
        cmd = "kubectl exec -it redis-master-0 -n key-value-store -- redis-cli -a cerebro DEL {}"
        try:
            for k in keys:
                run(cmd.format(k))
            for k in keys2:
                run(cmd.format(k))
        except Exception as e:
            print("Got error while cleaning up Key-Value Store: " + str(e))

        # clean up Controller
        try:
            cmd3 = "helm delete controller"
            run(cmd3, haltException=False)

            # delete PVCs
            v1.delete_namespaced_persistent_volume_claim(name="cerebro-repo-pvc", namespace=self.kube_namespace)
            v1.delete_namespaced_persistent_volume_claim(name="user-repo-pvc", namespace=self.kube_namespace)
            v1.delete_namespaced_persistent_volume_claim(name="cerebro-checkpoint-pvc", namespace=self.kube_namespace)
            v1.delete_namespaced_persistent_volume_claim(name="cerebro-config-pvc", namespace=self.kube_namespace)
            v1.delete_namespaced_persistent_volume_claim(name="cerebro-data-pvc", namespace=self.kube_namespace)
            v1.delete_namespaced_persistent_volume_claim(name="cerebro-metrics-pvc", namespace=self.kube_namespace)
        except Exception as e:
            print("Got error while cleaning up Controller: " + str(e))

        print("Cleaned up Controller")

        # clean up webapp
        try:
            run("helm delete webapp")
            print("Cleaned up Webapp")
        except Exception as e:
            print("Got error while cleaning up WebApp: " + str(e))

        # wait for all pods to shutdown
        pods_list = v1.list_namespaced_pod("cerebro")
        while pods_list.items != []:
            time.sleep(1)
            print("Waiting for pods to shutdown...")
            pods_list = v1.list_namespaced_pod("cerebro")

        print("Done")

    def deleteCluster(self):
        def _runCommands(fn, name):
            try:
                fn()
            except Exception as e:
                print("Ignoring command failure for - ", name)
                print(str(e))

        cluster_name = self.values_yaml["cluster"]["name"]

        cmd12 = "aws efs describe-file-systems --output json"
        out = json.loads(run(cmd12))
        fs_ids = []
        for i in out["FileSystems"]:
            fs_ids.append(i["FileSystemId"])
        print("Found file systems:", str(fs_ids))

        # delete all services
        def _delete_services():
            config.load_kube_config()
            v1 = client.CoreV1Api()
            services = v1.list_service_for_all_namespaces().items
            for service in services:
                v1.delete_namespaced_service(
                    name=service.metadata.name,
                    namespace=service.metadata.namespace,
                    body=client.V1DeleteOptions(propagation_policy='Foreground')
                )
                print("Deleted service: {} in namespace: {}".format(service.metadata.name, service.metadata.namespace))
        _runCommands(_delete_services, "servicesDelete")

        # delete the cluster
        def _deleteCluster():
            cmd1 = "eksctl delete cluster --name {}".format(cluster_name)
            run(cmd1, capture_output=False)
            print("Deleted the cluster")
        _runCommands(_deleteCluster, "clusterDelete")

        # Delete MountTargets
        def _deleteMountTargets():
            mt_ids = []
            for fs_id in fs_ids:
                cmd2 = """ aws efs describe-mount-targets \
                --file-system-id {} \
                --output json
                """.format(fs_id)
                out = json.loads(run(cmd2))
                mt_ids.extend([i["MountTargetId"] for i in out["MountTargets"]])

            cmd3 = """ aws efs delete-mount-target \
            --mount-target-id {}
            """
            for mt_id in mt_ids:
                run(cmd3.format(mt_id))
                print("Deleted MountTarget:", mt_id)
            print("Deleted all MountTargets")
        _runCommands(_deleteMountTargets, "deleteMountTarget")
        time.sleep(5)

        # delete FileSystem
        def _deleteFileSystem():
            for fs_id in fs_ids:
                cmd6 = """ aws efs delete-file-system \
                --file-system-id {}
                """.format(fs_id)
                run(cmd6)
                print("Deleted filesystem:", fs_id)
            print("Deleted all FileSystems")
        _runCommands(_deleteFileSystem, "deleteFileSystem")
        time.sleep(3)

        # delete SecurityGroup efs-nfs-sg
        def _deleteSecurityGroups():
            sg_gid = None
            cmd4 = "aws ec2 describe-security-groups"
            out = json.loads(run(cmd4))

            for sg in out["SecurityGroups"]:
                if sg["GroupName"] == "efs-nfs-sg":
                    sg_gid = sg["GroupId"]
                    break

            cmd5 = "aws ec2 delete-security-group --group-id {}".format(sg_gid)
            run(cmd5)
            print("Deleted SecurityGroup efs-nfs-sg")
        _runCommands(_deleteSecurityGroups, "deleteSecurityGroups")
        time.sleep(3)

        # delete Subnets
        def _deleteSubnets():
            cmd7 = " aws ec2 describe-subnets"
            cmd8 = "aws ec2 delete-subnet --subnet-id {}"
            out = json.loads(run(cmd7))
            for i in out["Subnets"]:
                if cluster_name in str(i["Tags"]):
                    run(cmd8.format(i["SubnetId"]))
                    print("Deleted Subnet:", i["SubnetId"])
            print("Deleted all Subnets")
        _runCommands(_deleteSubnets, "deleteSubnets")
        time.sleep(3)

        # delete VPC
        def _deleteVPC():
            cmd9 = " aws ec2 describe-vpcs"
            cmd10 = "aws ec2 delete-vpc --vpc-id {}"
            out = json.loads(run(cmd9))
            for i in out["Vpcs"]:
                if "Tags" in i and cluster_name in str(i["Tags"]):
                    run(cmd10.format(i["VpcId"]))
                    print("Deleted VPC:", i["VpcId"])
            print("Deleted all VPCs")
        _runCommands(_deleteVPC, "deleteVPC")
        time.sleep(3)

        # delete the cluster and wait
        def _deleteClusterWait():
            cmd1 = "eksctl delete cluster --name {}".format(cluster_name)
            run(cmd1, capture_output=False)
            print("delete_cluster_wait complete")
        _runCommands(_deleteClusterWait, "clusterDeleteWait")

        # delete Cloudformation Stack
        def _deleteCloudFormationStack():
            stack_name = "eksctl-" + cluster_name + "-cluster"
            cmd11 = "aws cloudformation delete-stack --stack-name {}".format(stack_name)
            run(cmd11)
            print("Deleted CloudFormation Stack")
        _runCommands(_deleteCloudFormationStack, "deleteCloudFormationStack")

        # delete S3 policy
        def _delete_policy():
            cmd = "aws sts get-caller-identity"
            account_id = json.loads(run(cmd))["Account"]
            arn = "arn:aws:iam::{}:policy/eks-s3-policy".format(account_id)
            detach_cmds = [
                "aws iam detach-role-policy --role-name {} --policy-arn {}".format("eks-s3-role",arn),
                "aws iam delete-policy --policy-arn {}".format(arn)
            ]
            for cmd in detach_cmds:
                run(cmd)
            print("Deleted S3 policy")
        _runCommands(_delete_policy, "deletePolicy")

        # delete S3 role
        def _delete_role():
            role_delete_cmd = "aws iam delete-role --role-name {}".format("eks-s3-role")
            run(role_delete_cmd)
            print("Deleted S3 role")
        _runCommands(_delete_role, "deleteRole")

        # delete OIDC provider
        def _delete_OIDC():
            oidc_arn_cmd = "aws iam list-open-id-connect-providers \
                --query 'OpenIDConnectProviderList[].Arn' \
                --region {} \
                --output text".format(self.values_yaml["cluster"]["region"])
            oidc_delete_cmd = "aws iam delete-open-id-connect-provider --open-id-connect-provider-arn {}"
            oidc_arn = run(oidc_arn_cmd)
            run(oidc_delete_cmd.format(oidc_arn))
            print("Deleted OIDC provider")
        _runCommands(_delete_OIDC, "deleteOIDC")

        print("Cluster delete complete")

    def pauseCluster(self):
        config.load_kube_config()
        v1 = client.CoreV1Api()

        # cleanUp cluster
        print("Clearing Cerebro resources...")
        self.cleanUp()

        # scale down cluster to 0 nodes
        print("Scaling down cluster...")
        cluster_name = self.values_yaml["cluster"]["name"]
        cmd1 = "eksctl scale nodegroup --cluster {} --name ng-worker --nodes 0 --nodes-max 1 --nodes-min 0 --wait"
        cmd2 = "eksctl scale nodegroup --cluster {} --name ng-controller --nodes 0 --nodes-max 1 --nodes-min 0 --wait"
        run(cmd1.format(cluster_name), capture_output=False, haltException=False)
        run(cmd2.format(cluster_name), capture_output=False, haltException=False)

        # wait for desired number of nodes
        current_nodes = None
        while current_nodes != 0:
            nodes_list = v1.list_node().items
            current_nodes = len(nodes_list)
            time.sleep(1)

        print("Cluster paused!")

    def resumeCluster(self):
        config.load_kube_config()
        v1 = client.CoreV1Api()

        # scale up cluster to num_nodes
        print("Scaling up cluster...")
        cluster_name = self.values_yaml["cluster"]["name"]
        num_nodes = self.values_yaml["cluster"]["numWorkers"]
        cmd1 = "eksctl scale nodegroup --cluster {0} --name ng-controller --nodes 1 --nodes-max 1 --nodes-min 0 --wait"
        cmd2 = "eksctl scale nodegroup --cluster {0} --name ng-worker --nodes {1} --nodes-max {1} --nodes-min 0 --wait"
        run(cmd1.format(cluster_name), capture_output=False, haltException=False)
        run(cmd2.format(cluster_name, num_nodes), capture_output=False, haltException=False)

        # wait for desired num_nodes
        current_nodes = 0
        while current_nodes != num_nodes + 1:
            nodes_list = v1.list_node().items
            current_nodes = len(nodes_list)
            time.sleep(1)

        # install Cerebro
        print("Creating Cerebro objects...")
        # self.installCerebro()

        # load fabric connections
        self.initializeFabric()

        # cmds = [
        #     "iptables -P INPUT ACCEPT",
        #     "iptables -P FORWARD ACCEPT",
        #     "iptables -P OUTPUT ACCEPT",
        #     "iptables -F"
        # ]
        # for cmd in cmds:
        #     self.conn.sudo(cmd)
        #     self.s.sudo(cmd)

        print("Cluster resumed!")

    def testing(self):
        username = run("whoami")

    # call the below functions from CLI
    def createCluster(self):
        from datetime import timedelta

        region = self.values_yaml["cluster"]["region"]

        with open("init_cluster/eks_cluster_template.yaml", 'r') as yamlfile:
            eks_cluster_yaml = yamlfile.read()

        worker_instance_type = "\n  - " + "\n  - ".join(self.values_yaml["cluster"]["workerInstances"])
        public_key_name = str(os.path.abspath(self.values_yaml["creds"]["pemPath"])).split("/")[-1].split(".")[0]

        # get ami value for kubernetes version
        cmd = 'aws ssm get-parameter --name /aws/service/eks/optimized-ami/{}/amazon-linux-2-gpu/recommended/image_id --region {} --query "Parameter.Value" --output text'
        ami = run(cmd.format(self.values_yaml["cluster"]["kubernetesVersion"], region))

        # get set of availability zones that support all instance types
        cmd1 = "aws ec2 describe-instance-type-offerings --location-type availability-zone  --filters Name=instance-type,Values={} --region {} | jq -r '.InstanceTypeOfferings[].Location'"
        availbility_zones = []
        for i in self.values_yaml["cluster"]["workerInstances"]:
            zones = run(cmd1.format(i, region)).split()
            availbility_zones.append(set(zones))
        availbility_zones = "\n  - " + "\n  - ".join(set.intersection(*availbility_zones))

        eks_cluster_yaml = eks_cluster_yaml.replace("{{ ami }}", ami)
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ publicKeyName }}", public_key_name)
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ availabilityZones }}", availbility_zones)
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ desiredCapacity }}", str(self.num_workers))
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ name }}", self.values_yaml["cluster"]["name"])
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ worker.instanceType }}", worker_instance_type)
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ region }}", self.values_yaml["cluster"]["region"])
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ volumeSize }}", str(self.values_yaml["cluster"]["volumeSize"]))
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ workerSpot }}", str(self.values_yaml["cluster"]["workerSpotInstance"]))
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ kubernetesVersion }}", str(self.values_yaml["cluster"]["kubernetesVersion"]))
        eks_cluster_yaml = eks_cluster_yaml.replace("{{ controller.instanceType }}", self.values_yaml["cluster"]["controllerInstance"])

        with open("init_cluster/eks_cluster.yaml", "w") as yamlfile:
            yamlfile.write(eks_cluster_yaml)

        try:
            start = time.time()
            cmd = "eksctl create cluster -f ./init_cluster/eks_cluster.yaml"
            subprocess.run(cmd, shell=True, text=True)
            end = time.time()
            print("Created cluster successfully")
            print("Time taken to create cluster:", str(timedelta(seconds=end - start)))
        except Exception as e:
            print("Couldn't create the cluster")
            print(str(e))

        # create Cerebro namespace
        config.load_kube_config()
        v1 = client.CoreV1Api()
        metadata = client.V1ObjectMeta(name=self.kube_namespace)
        spec = client.V1NamespaceSpec()
        namespace = client.V1Namespace(metadata=metadata, spec=spec)
        v1.create_namespace(namespace)
        print("Created Cerebro namespace")

        # add EFS storage
        self.addStorage()

        # add Local DNS Cache
        self.addLocalDNSCache()

        # install Prometheus and Grafana
        self.installMetricsMonitor()

        # install Key-Value Store
        self.installKeyValueStore()

        # initialize basic cerebro components
        self.initCerebro()

        # install Cerebro
        self.installCerebro()

    def installCerebro(self):
        # creates Controller
        self.createController()

        # create webapp
        self.createWebApp()

        # create Workers
        self.createWorkers()

        url = self.values_yaml["cluster"]["networking"]["publicDNSName"]
        port = self.values_yaml["webApp"]["uiNodePort"]

        time.sleep(5)
        print("You can access the cluster using this URL:")
        print("http://{}:{}".format(url, port))


if __name__ == '__main__':
    fire.Fire(CerebroInstaller)
