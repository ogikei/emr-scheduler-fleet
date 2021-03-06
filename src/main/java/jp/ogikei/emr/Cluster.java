package jp.ogikei.emr;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;

import org.json.JSONArray;
import org.json.JSONObject;

public class Cluster {

  private static final Logger logger = Logger.getLogger(Cluster.class.getName());

  private String clusterName;
  private String emrVersion;
  private List<Application> applications = new ArrayList<>();
  private String serviceRole;
  private String jobFlowRole;
  private String keyName;
  private String masterSecurityGroup;
  private String slaveSecurityGroup;

  public Cluster(JSONObject jsonObject) {
    JSONObject emrJSONObject = jsonObject.getJSONObject("emr");
    clusterName = emrJSONObject.getString("clusterName");
    emrVersion = emrJSONObject.getString("emrVersion");
    JSONArray appsJSONArray = emrJSONObject.getJSONArray("applications");

    // debug
    System.out.println(clusterName);
    System.out.println(emrVersion);

    System.out.println(appsJSONArray.getString(0));
    System.out.println(appsJSONArray.getString(1));

    for (Integer i = 0; i < appsJSONArray.length(); i++) {
      Application application = new Application().withName(appsJSONArray.getString(i));
      applications.add(i, application);
    }
    serviceRole = emrJSONObject.getString("serviceRole");
    jobFlowRole = emrJSONObject.getString("jobFlowRole");
    keyName = emrJSONObject.getString("keyName");

    JSONObject resourcesObject = emrJSONObject.getJSONObject("resources");
    JSONObject masterJSONObject = resourcesObject.getJSONObject("master");
    masterSecurityGroup = masterJSONObject.getString("securityGroup");

    JSONObject slaveJSONObject = resourcesObject.getJSONObject("core");
    slaveSecurityGroup = slaveJSONObject.getString("securityGroup");
  }

  public void createEMRCluster(
      AmazonElasticMapReduce emrClient, List<InstanceFleetConfig> instanceFleetConfigs) {
    // TODO: 2017/07/10 add configuration if needed
    //    Map<String,String> hiveProperties = new HashMap<String,String>();
    //    hiveProperties.put("hive.join.emit.interval","1000");
    //    hiveProperties.put("hive.merge.mapfiles","true");
    //    Configuration config = new Configuration()
    //        .withClassification("hive-site")
    //        .withProperties(hiveProperties);

    RunJobFlowRequest jobFlowRequest = new RunJobFlowRequest()
        .withName(clusterName)
        .withReleaseLabel(emrVersion)
        .withApplications(applications)
        .withConfigurations()
        .withServiceRole(serviceRole)
        .withJobFlowRole(jobFlowRole)
        .withInstances(new JobFlowInstancesConfig()
            .withEc2KeyName(keyName)
            .withKeepJobFlowAliveWhenNoSteps(true)
            .withAdditionalMasterSecurityGroups(masterSecurityGroup)
            .withAdditionalSlaveSecurityGroups(slaveSecurityGroup)
            .withInstanceFleets(instanceFleetConfigs)
            .withEc2SubnetIds("subnet-94dfc2e2"));

    RunJobFlowResult jobFlowResult = emrClient.runJobFlow(jobFlowRequest);
    logger.info("requested job flow. JobFlowID: " + jobFlowResult.getJobFlowId());
  }

}
