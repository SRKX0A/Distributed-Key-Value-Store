# Distributed-Key-Value-Store
Distributed key-value store written in Java 11

Build with ```ant build``` in project root, clean artifacts with ```ant clean```, run unit tests with ```ant test```.

To start a client: ```java -jar m4-client.jar -p <NOTIFICATION-PORT>```, e.x. ```java -jar m4-client.jar -p 6000```

To start an External Configuration Service (ECS): ```java -jar m4-ecs.jar -p <LISTEN-PORT>```, e.x. ```java -jar m4-ecs.jar -p 7000```

To start a server: ```java -jar m4-server.jar -p <LISTEN-PORT> -b <ECS-IP-ADDRESS:ECS-PORT> -d <STORAGE-DIRECTORY> -t <TIME-TO-REPLICATE-IN-MILLISECONDS>```, e.x. ```java -jar m4-server.jar -p 8000 -b 1.2.3.4:7000 -d storage_dir -t 10000```.

Multiple servers can be launched on the same/different machine; use the same ECS for server configuration purposes. Multiple clients can be launched on any machine as well.
