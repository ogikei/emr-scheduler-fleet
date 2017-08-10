package jp.ogikei.ec2.spot;

import com.amazonaws.services.elasticmapreduce.model.AddInstanceFleetRequest;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleet;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetModifyConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetType;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.ListInstanceFleetsRequest;
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceFleetRequest;
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
        .withInstanceType(masterJSON.getString("type"))
        .withBidPrice(masterJSON.getString("bidPrice"))
        .withWeightedCapacity(masterJSON.getInt("targetSpotCapacity"));

    instanceFleetConfig
        .withTargetSpotCapacity(slaveJSON.getInt("targetSpotCapacity"))
        .withTargetOnDemandCapacity(slaveJSON.getInt("targetOnDemandCapacity"))
        .withInstanceFleetType(InstanceFleetType.CORE)
        .withInstanceTypeConfigs(instanceTypeConfig);
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
  }

}