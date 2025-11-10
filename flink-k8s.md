### 步骤 1：创建 Namespace
kubectl create namespace flink

### 步骤 2：创建 ServiceAccount 和 RBAC
创建 flink-rbac.yaml：
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: flink
  namespace: flink
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: flink
  namespace: flink
rules:
  - apiGroups: [""]
    resources: ["pods", "configmaps"]
    verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: flink
  namespace: flink
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: flink
subjects:
  - kind: ServiceAccount
    name: flink
    namespace: flink
```

应用：
kubectl apply -f flink-rbac.yaml

### 步骤 3：创建 ConfigMap
创建 flink-config.yaml：
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
  namespace: flink
data:
  flink-conf.yaml: |
    jobmanager.rpc.address: flink-jobmanager
    taskmanager.numberOfTaskSlots: 2
    blob.server.port: 6124
    jobmanager.rpc.port: 6123
    taskmanager.rpc.port: 6122
    queryable-state.proxy.ports: 6125
    jobmanager.memory.process.size: 1024m
    taskmanager.memory.process.size: 1024m
  log4j-console.properties: |
    rootLogger.level = INFO
    rootLogger.appenderRef.console.ref = ConsoleAppender
    appender.console.name = ConsoleAppender
    appender.console.type = CONSOLE
    appender.console.layout.type = PatternLayout
    appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
```

应用：
kubectl apply -f flink-config.yaml

### 步骤 4：部署 JobManager
创建 jobmanager-deployment.yaml：
```yaml
apiVersion: v1
kind: Service
metadata:
  name: flink-jobmanager
  namespace: flink
spec:
  type: ClusterIP
  ports:
  - name: rpc
    port: 6123
  - name: blob-server
    port: 6124
  - name: webui
    port: 8081
  selector:
    app: flink
    component: jobmanager
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
  namespace: flink
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink
      component: jobmanager
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      serviceAccountName: flink
      containers:
      - name: jobmanager
        image: flink:1.18
        args: ["jobmanager"]
        ports:
        - containerPort: 6123
          name: rpc
        - containerPort: 6124
          name: blob-server
        - containerPort: 8081
          name: webui
        env:
        - name: FLINK_PROPERTIES
          value: |
            jobmanager.rpc.address: flink-jobmanager
        volumeMounts:
        - name: flink-config-volume
          mountPath: /opt/flink/conf
      volumes:
      - name: flink-config-volume
        configMap:
          name: flink-config
```
应用：
kubectl apply -f jobmanager-deployment.yaml

### 步骤 5：部署 TaskManager
创建 taskmanager-deployment.yaml：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-taskmanager
  namespace: flink
spec:
  replicas: 2
  selector:
    matchLabels:
      app: flink
      component: taskmanager
  template:
    metadata:
      labels:
        app: flink
        component: taskmanager
    spec:
      serviceAccountName: flink
      containers:
      - name: taskmanager
        image: flink:1.18
        args: ["taskmanager"]
        ports:
        - containerPort: 6122
          name: rpc
        env:
        - name: FLINK_PROPERTIES
          value: |
            jobmanager.rpc.address: flink-jobmanager
            taskmanager.numberOfTaskSlots: 2
        volumeMounts:
        - name: flink-config-volume
          mountPath: /opt/flink/conf
      volumes:
      - name: flink-config-volume
        configMap:
          name: flink-config
```

应用：
kubectl apply -f taskmanager-deployment.yaml

### 步骤 6：验证部署
1. 查看所有资源
```shell
kubectl get all -n flink
```

2. 查看 pods 状态
```shell
kubectl get pods -n flink
```

3. 预期输出类似
```shell
NAME                                  READY   STATUS    RESTARTS   AGE
flink-jobmanager-xxxxxxxxx-xxxxx      1/1     Running   0          1m 
flink-taskmanager-xxxxxxxxx-xxxxx     1/1     Running   0          1m
flink-taskmanager-xxxxxxxxx-xxxxx     1/1     Running   0          1m
```

### 步骤 7：访问 Web UI

kubectl port-forward -n flink service/flink-jobmanager 8081:8081

然后在浏览器访问：http://localhost:8081

快速部署脚本（可选）
如果你想一次性部署所有资源，可以创建一个 deploy-flink.sh 脚本：
```shell
#!/bin/bash

# 创建 namespace
kubectl create namespace flink

# 应用所有配置
kubectl apply -f flink-rbac.yaml
kubectl apply -f flink-config.yaml
kubectl apply -f jobmanager-deployment.yaml
kubectl apply -f taskmanager-deployment.yaml

# 等待 pods 就绪
echo "等待 Flink 集群启动..."
kubectl wait --for=condition=ready pod -l app=flink -n flink --timeout=300s

# 显示状态
kubectl get pods -n flink

echo "部署完成！使用以下命令访问 Web UI："
echo "kubectl port-forward -n flink service/flink-jobmanager 8081:8081"
```
