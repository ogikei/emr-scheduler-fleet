package jp.ogikei.ec2.spot;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetType;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;

import org.json.JSONObject;

public class Fleet {

  private static final Logger logger = Logger.getLogger(Fleet.class.getName());

  private JSONObject masterJSONObject;
  private JSONObject coreJSONObject;
  private JSONObject taskJSONObject;

  private InstanceTypeConfig masterInstanceTypeConfig;
  private InstanceTypeConfig coreInstanceTypeConfig;
  private InstanceTypeConfig taskInstanceTypeConfig;

  private InstanceFleetConfig masterInstanceFleetConfig;
  private InstanceFleetConfig coreInstanceFleetConfig;
  private InstanceFleetConfig taskInstanceFleetConfig;

  public Fleet(JSONObject jsonObject) {
    masterJSONObject =
        jsonObject.getJSONObject("emr").getJSONObject("resources").getJSONObject("master");
    coreJSONObject =
        jsonObject.getJSONObject("emr").getJSONObject("resources").getJSONObject("core");
    taskJSONObject =
        jsonObject.getJSONObject("emr").getJSONObject("resources").getJSONObject("task");
  }

  private void setInstanceTypeConfig() {
    masterInstanceTypeConfig = new InstanceTypeConfig()
        .withInstanceType(masterJSONObject.getString("type"))
        .withBidPrice(masterJSONObject.getString("bidPrice"));

    coreInstanceTypeConfig = new InstanceTypeConfig()
        .withInstanceType(coreJSONObject.getString("type"))
        .withBidPrice(coreJSONObject.getString("bidPrice"));

    taskInstanceTypeConfig = new InstanceTypeConfig()
        .withInstanceType(taskJSONObject.getString("type"))
        .withBidPrice(taskJSONObject.getString("bidPrice"));
  }

  private void setInstanceFleetConfig() {
    masterInstanceFleetConfig = new InstanceFleetConfig()
        .withInstanceFleetType(InstanceFleetType.MASTER)
        .withTargetSpotCapacity(masterJSONObject.getInt("targetSpotCapacity"))
        .withInstanceTypeConfigs(masterInstanceTypeConfig);

    coreInstanceFleetConfig = new InstanceFleetConfig()
        .withInstanceFleetType(InstanceFleetType.CORE)
        .withTargetSpotCapacity(coreJSONObject.getInt("targetSpotCapacity"))
        .withInstanceTypeConfigs(coreInstanceTypeConfig);

    if (taskJSONObject.getInt("targetSpotCapacity") != 0) {
      taskInstanceFleetConfig = new InstanceFleetConfig()
          .withInstanceFleetType(InstanceFleetType.TASK)
          .withTargetSpotCapacity(taskJSONObject.getInt("targetSpotCapacity"))
          .withInstanceTypeConfigs(taskInstanceTypeConfig);
    }
  }

  public List<InstanceFleetConfig> createInstanceFleetConfigs() {
    this.setInstanceTypeConfig();
    this.setInstanceFleetConfig();
    List<InstanceFleetConfig> instanceFleetConfigs = new ArrayList<InstanceFleetConfig>() {
      {
        add(masterInstanceFleetConfig);
        add(coreInstanceFleetConfig);
        add(taskInstanceFleetConfig);
      }
    };
    instanceFleetConfigs.removeIf(Objects::isNull);

    return instanceFleetConfigs;
  }

}
