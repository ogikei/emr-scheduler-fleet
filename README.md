# emr-scheduler-fleet

## How to Use

```console
$ gradle fatJar
$ java -jar -Dregion=<region-name> -Dprofile=<your-profile> -DbucketName=<s3-bucket-name> -Dkey=<config-file> build/libs/emr-scheduler-fleet-1.0-SNAPSHOT.jar
```

## Configuration

```json
{
  "emr": {
    "clusterName": "EMR Test",
    "emrVersion": "emr-5.6.0",
    "applications": [
      "hive",
      "spark"
    ],
    "serviceRole": "EMR_DefaultRole",
    "jobFlowRole": "EMR_EC2_DefaultRole",
    "keyName": "emr-test",
    "resources": {
      "master": {
        "name": "Master",
        "type": "m4.large",
        "securityGroup": "sg-df893eb9",
        "bidPrice": "0.04",
        "targetSpotCapacity": 1,
        "targetOnDemandCapacity": 0,
        "timeoutDurationMinutes": 30,
        "timeoutAction": "TERMINATE_CLUSTER"
      },
      "core": {
        "name": "Core",
        "type": "m4.large",
        "securityGroup": "sg-818a3de7",
        "bidPrice": "0.04",
        "targetSpotCapacity": 2,
        "targetOnDemandCapacity": 0,
        "timeoutDurationMinutes": 30,
        "timeoutAction": "TERMINATE_CLUSTER"
      },
      "task": {
        "name": "Task",
        "type": "m4.large",
        "securityGroup": "sg-818a3de7",
        "bidPrice": "0.04",
        "targetSpotCapacity": 0,
        "targetOnDemandCapacity": 0,
        "timeoutDurationMinutes": 30,
        "timeoutAction": "TERMINATE_CLUSTER"
      }
    }
  }
}
```
