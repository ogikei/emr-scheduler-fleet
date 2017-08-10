package jp.ogikei.auth;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;

public class Credential {

  public static ProfileCredentialsProvider credential;
  public static ClientConfiguration clientConfig;

  public static void createCredential(String profile) {
    credential = new ProfileCredentialsProvider(profile);
  }

  public static void createConfiguration() {
    clientConfig = new ClientConfiguration().withProtocol(Protocol.HTTP);
  }

}
