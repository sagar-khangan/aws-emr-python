import time
import sys
import boto3
import yaml
import webbrowser




def create_cluster(conn):
    try:
        applications = []

        for i in cfg['applications']:
            temp = {'Name': i}
            applications.append(temp)
        
        bootstrap_actions = [
                {
                    'Name': 'Install Anaconda and link with pyspark',
                    'ScriptBootstrapAction': {
                        'Path': bootstrap_file,
                    }
                },
            ]
                
        instances = {
                'MasterInstanceType': master_instance,
                'SlaveInstanceType': slave_instance,
                'InstanceCount': instance_count,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName' :ec2_key,
                'EmrManagedMasterSecurityGroup': master_sg,
                'EmrManagedSlaveSecurityGroup': slave_sg,
            }

        cluster_id = conn.run_job_flow(
            Name=name,
            LogUri=log_uri,
            ReleaseLabel= emr_version,
            Instances= instances,
            Applications=applications,
            BootstrapActions =bootstrap_actions,
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            EbsRootVolumeSize=ebs_size,
            
        )

        return cluster_id

    except Exception as e:
        print(e, "Exception")

def poll_cluster(conn,cluster):
    try:
        while True:
            start_resp = conn.list_clusters(
                ClusterStates=[
                    'STARTING'
                ]
            )
            if len(start_resp['Clusters'])>0:
                print("starting ", start_resp)
            boot_resp = conn.list_clusters(
                ClusterStates=[
                    'BOOTSTRAPPING'
                ]
            )
            if len(boot_resp['Clusters'])>0:
                print("bootstrapping", boot_resp)
            run_resp = conn.list_clusters(
                ClusterStates=[
                    'WAITING'
                ]
            )

            if len(run_resp['Clusters'])>0:
                # print("Running", run_resp)
                instance = conn.list_instances(
                    ClusterId = cluster_id,
                    InstanceGroupTypes=[
                        'MASTER'
                    ],
                    # InstanceFleetType='MASTER',
                    InstanceStates=[
                       'RUNNING'
                    ]
                )


                zeppelin_url = "http://"+instance['Instances'][0]['PublicDnsName']+":"+zeppelin_port+"/#/"

                webbrowser.open(zeppelin_url)
                return
            time.sleep(60)
    except Exception as e:
        print(e,"Exception")

def terminate_cluster(id,conn):
    try:
        resp = conn.terminate_job_flows(
            JobFlowIds=[id]
        )
        print (resp)
    except Exception as e:
        print(e, "EXception")


if __name__== "__main__":

    try:
        with open('config.yml') as json_data_file:
            cfg = yaml.load(json_data_file)

        name = cfg['name']
        bootstrap_file = cfg['bootstrap_file']
        log_uri= cfg['log_uri']
        region= cfg['region']
        ec2_key = cfg['ec2-key']
        emr_version = str(cfg['emr'])
        master_instance = cfg['master_instance']
        slave_instance = cfg['slave_instance']
        instance_count = cfg['instance_count']
        ebs_size = cfg['ebs_size']
        master_sg = cfg['master_sg']
        slave_sg = cfg['slave_sg']
        zeppelin_port = str(cfg['port']['Zeppelin'])

        conn = boto3.client('emr', region_name=region)

        if sys.argv[1] == 'create':
            cluster_id = create_cluster(conn)
            poll_cluster(conn,cluster_id)
        elif sys.argv[1] == 'terminate':
            terminate_cluster(sys.argv[2],conn)
        else:
            print ("specify argument create or terminate <id>")

    except Exception as e:
        print(e, "Exception")
