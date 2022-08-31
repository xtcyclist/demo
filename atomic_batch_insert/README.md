A simple demo of inserting a batch of vertices. If any insert query fails, all execeuted queries are rolledback.

To run this demo, start a NebulaGraph cluster, import the nba datatest, fill the ip address and port of one nebula-graphd into the script (line 104 and 105). If you are building and deploying NebulaGraph from sources, in the /nebula/tests folder, run ```make up``` to start up the service and import the dataset automatically.

To execute:
```
python3 ./atomic_batch_insert_demo.py
```
