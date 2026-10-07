package vai.lakehouse.dakmock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Mock DAK: chỉ phục vụ API lấy key cho sql-engine/SparkApplication (docs/DAK_API_SPEC.md mục 5). */
@SpringBootApplication
@ConfigurationPropertiesScan
public class DakMockApplication {

  public static void main(String[] args) {
    SpringApplication.run(DakMockApplication.class, args);
  }
}
