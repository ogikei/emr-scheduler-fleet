package jp.ogikei;

import java.io.InputStream;

import jp.ogikei.auth.Credential;
import jp.ogikei.ec2.spot.Fleet;
import jp.ogikei.emr.Cluster;
import jp.ogikei.s3.Bucket;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.json.JSONObject;

class Main {

  public static void main(String... args) {
    String region = System.getProperty("region");
    String profile = System.getProperty("profile");
    String bucketName = System.getProperty("bucketName");
    String key = System.getProperty("key");

    Credential.createCredential(profile);
    Credential.createConfiguration();

    AmazonS3 s3Client = AmazonS3ClientBuilder
        .standard()
        .withCredentials(Credential.credential)
        .withClientConfiguration(Credential.clientConfig)
        .withRegion(region)
        .build();

    Bucket bucket = new Bucket();
    InputStream inputStream = bucket.getObject(s3Client, bucketName, key);
    JSONObject jsonObject = bucket.getJSONObjectFromS3(inputStream);

    Fleet fleet = new Fleet(jsonObject);

    AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder
        .standard()
        .withCredentials(Credential.credential)
        .withClientConfiguration(Credential.clientConfig)
        .withRegion(region)
        .build();

    Cluster cluster = new Cluster(jsonObject);
    cluster.createEMRCluster(emrClient, fleet.createInstanceFleetConfigs());
  }

}
