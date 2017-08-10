package jp.ogikei.ec2.spot;

import com.amazonaws.services.elasticmapreduce.model.AddInstanceFleetRequest;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetModifyConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetType;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsRequest;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceFleetRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import org.json.JSONObject;

public class Fleet {

  private static final Logger logger = Logger.getLogger(Fleet.class.getName());

  private InstanceFleetConfig instanceFleetConfig = new InstanceFleetConfig();
  private InstanceFleetModifyConfig instanceFleetModifyConfig = new InstanceFleetModifyConfig();

  public Fleet(JSONObject jsonObject) {
    JSONObject masterJSON =
        jsonObject.getJSONObject("emr").getJSONObject("resources").getJSONObject("master");
    JSONObject slaveJSON =
        jsonObject.getJSONObject("emr").getJSONObject("resources").getJSONObject("slave");

    ListInstanceFleetsRequest listInstanceFleetsRequest = new ListInstanceFleetsRequest();

    InstanceTypeConfig instanceTypeConfig = new InstanceTypeConfig()
        .withConfigurations()
        .withInstanceType(masterJSON.getString("type"))
        .withBidPrice(masterJSON.getString("bidPrice"))
        .withWeightedCapacity(masterJSON.getInt("targetSpotCapacity"));

    InstanceTypeConfig instanceTypeConfig2 = new InstanceTypeConfig()
        .withInstanceType(masterJSON.getString("type"))
        .withBidPrice(masterJSON.getString("bidPrice"))
        .withWeightedCapacity(masterJSON.getInt("targetSpotCapacity"));

    List<InstanceTypeConfig> instanceTypeConfigs = new ArrayList<>();
    instanceTypeConfigs.add(instanceTypeConfig);
    instanceTypeConfigs.add(instanceTypeConfig2);

    instanceFleetConfig
        .withTargetSpotCapacity(slaveJSON.getInt("targetSpotCapacity"))
        .withTargetOnDemandCapacity(slaveJSON.getInt("targetOnDemandCapacity"))
        .withInstanceFleetType(InstanceFleetType.CORE)
        .withInstanceTypeConfigs(instanceTypeConfigs);

    instanceFleetConfig
        .withTargetSpotCapacity(slaveJSON.getInt("targetSpotCapacity"))
        .withTargetOnDemandCapacity(slaveJSON.getInt("targetOnDemandCapacity"))
        .withInstanceFleetType(InstanceFleetType.CORE)
        .withInstanceTypeConfigs(instanceTypeConfigs);
  }

  public AddInstanceFleetRequest createAddInstanceFleetRequest() {
    return new AddInstanceFleetRequest()
        .withInstanceFleet(instanceFleetConfig)
        .withClusterId("");
  }

  public ModifyInstanceFleetRequest createModifyInstanceFleetRequest() {
    return new ModifyInstanceFleetRequest()
        .withInstanceFleet(instanceFleetModifyConfig)
        .withClusterId("");

//    SpotSpecification -> SpotProvisioningSpecification
  }

}

// http://docs.aws.amazon.com/emr/latest/DeveloperGuide/emr-instance-fleet.html
// only can use units via api
