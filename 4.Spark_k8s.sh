--get the latest code
git clone https://github.com/ibm-developer-skills-network/fgskh-new_horizons.git

--change directory
cd fgskh-new_horizons

--add an alias to for less typing
alias k='kubectl'

--save the namespace
my_namespace=$(kubectl config view --minify -o jsonpath='{..namespace}')

--install the spark pod
k apply -f spark/pod_spark.yaml

--check the status of pod (Kubernetes automatically RESTARTS failed pods )
k get po

--if you see this like this, you have to delete the pod and start again(usually happens when the image registry is unreliable or offline)
'''
NAME   READY   STATUS              RESTARTS   AGE  
spark  0/2     ImagePullBackOff    0          29s
'''
k delete po spark

--command exec is told to provide access to the container called spark (-c). With – we execute a command
k exec spark -c spark  -- echo "Hello from inside the container"

--submits the SparkPi sample application to the cluster
k exec spark -c spark -- ./bin/spark-submit \
--master k8s://http://127.0.0.1:8001 \
--deploy-mode cluster \
--name spark-pi \
--class org.apache.spark.examples.SparkPi \
--conf spark.executor.instances=1 \
--conf spark.kubernetes.container.image=romeokienzler/spark-py:3.1.2 \
--conf spark.kubernetes.executor.request.cores=0.2 \
--conf spark.kubernetes.executor.limit.cores=0.3 \
--conf spark.kubernetes.driver.request.cores=0.2 \
--conf spark.kubernetes.driver.limit.cores=0.3 \
--conf spark.driver.memory=512m \
--conf spark.kubernetes.namespace=${my_namespace} \
local:///opt/spark/examples/jars/spark-examples_2.12-3.1.2.jar \
10

'''
./bin/spark-submit is the command to submit applications to a Apache Spark cluster
–master k8s://http://127.0.0.1:8001 is the address of the Kubernetes API server - the way kubectl but also the Apache Spark native Kubernetes scheduler interacts with the Kubernetes cluster
–name spark-pi provides a name for the job and the subsequent Pods created by the Apache Spark native Kubernetes scheduler are prefixed with that name
–class org.apache.spark.examples.SparkPi provides the canonical name for the Spark application to run (Java package and class name)
–conf spark.executor.instances=1 tells the Apache Spark native Kubernetes scheduler how many Pods it has to create to parallelize the application. Note that on this single node development Kubernetes cluster increasing this number doesn’t make any sense (besides adding overhead for parallelization)
–conf spark.kubernetes.container.image=romeokienzler/spark-py:3.1.2 tells the Apache Spark native Kubernetes scheduler which container image it should use for creating the driver and executor Pods. This image can be custom build using the provided Dockerfiles in kubernetes/dockerfiles/spark/ and bin/docker-image-tool.sh in the Apache Spark distribution
–conf spark.kubernetes.executor.limit.cores=0.3 tells the Apache Spark native Kubernetes scheduler to set the CPU core limit to only use 0.3 core per executor Pod
–conf spark.kubernetes.driver.limit.cores=0.3 tells the Apache Spark native Kubernetes scheduler to set the CPU core limit to only use 0.3 core for the driver Pod
–conf spark.driver.memory=512m tells the Apache Spark native Kubernetes scheduler to set the memory limit to only use 512MBs for the driver Pod
–conf spark.kubernetes.namespace=${my_namespace} tells the Apache Spark native Kubernetes scheduler to set the namespace to my_namespace environment variable that we set before.
local:///opt/spark/examples/jars/spark-examples_2.12-3.1.2.jar indicates the jar file the application is contained in. Note that the local:// prefix addresses a path within the container images provided by the spark.kubernetes.container.image option. Since we’re using a jar provided by the Apache Spark distribution this is not a problem, otherwise the spark.kubernetes.file.upload.path option has to be set and an appropriate storage subsystem has to be configured, as described in the documentation
10 tells the application to run for 10 iterations, then output the computed value of Pi
Please see the documentation for a full list of available parameters.
'''

--check the time elapsed
kubectl logs spark-pi-6f62d17a800beb3e-driver |grep "Job 0 finished:"

--check the pi result
kubectl logs spark-pi-8e667c88b3d15c8d-driver |grep "Pi is roughly "

'''
Now you can adjust the cores or iterations and check the result again
'''
