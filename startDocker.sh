rm -r bigdata
mkdir bigdata
cd bigdata
sudo docker pull joway/hadoop-cluster 
git clone https://github.com/joway/hadoop-cluster-docker 
sudo docker network create --driver=bridge hadoop 
cd hadoop-cluster-docker
./start-container.sh
./start-hadoop.sh
