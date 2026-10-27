package vai.lakehouse.dakmock.deid;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vai.lakehouse.dakmock.config.DeidProperties.JobEnv;

/**
 * E10 (docs/dak-deid-flow/JOB_ENV_PLAN.md): danh sách env của mock ({@code dak.deid.job-env}) và bảng {@code JOB_ENV} trong DAG mẫu
 * phải khớp nhau, nếu không phiếu qua được mock sẽ bị DAG từ chối (hoặc ngược lại). Test đọc thẳng file DAG nên đổi một bên mà
 * quên bên kia thì test đỏ. Chỉ kiểm được bản DAG trong repo, không kiểm được DAG đã triển khai trên Airflow.
 */
class JobEnvDagDriftTest {

  private static final Path DAG = Path.of("../airflow/dags/sample_dag_deid_pseudonymize_table.py");
  private static final Pattern ENTRY = Pattern.compile(
      "\"([A-Z][A-Z0-9_]*)\":\\s*\\((None|\"([^\"]*)\"),\\s*r\"([^\"]*)\"\\)");

  /** Một dòng của JOB_ENV trong DAG: tên, mặc định ({@code null} nếu Python ghi None), quy tắc. */
  private record DagEntry(String name, String defaultValue, String rule) {}

  private static List<DagEntry> dagJobEnv() throws IOException {
    String source = Files.readString(DAG, StandardCharsets.UTF_8);
    int start = source.indexOf("JOB_ENV = {");
    int end = source.indexOf("\n}", start);
    assertThat(start).as("JOB_ENV block exists in the DAG").isGreaterThanOrEqualTo(0);
    assertThat(end).isGreaterThan(start);
    Matcher m = ENTRY.matcher(source.substring(start, end));
    List<DagEntry> entries = new ArrayList<>();
    while (m.find()) {
      entries.add(new DagEntry(m.group(1), "None".equals(m.group(2)) ? null : m.group(3), m.group(4)));
    }
    return entries;
  }

  @Test
  @DisplayName("the DAG lists exactly the env names of the default dak.deid.job-env, in the same order")
  void sameNames() throws IOException {
    List<String> mock = JobEnv.defaults().allowed().stream().map(JobEnv.Entry::name).toList();

    assertThat(dagJobEnv()).extracting(DagEntry::name).containsExactlyElementsOf(mock);
  }

  @Test
  @DisplayName("every env has the same value rule in the DAG and in the mock")
  void sameRules() throws IOException {
    List<DagEntry> dag = dagJobEnv();
    List<JobEnv.Entry> mock = JobEnv.defaults().allowed();

    assertThat(dag).hasSameSizeAs(mock);
    for (int i = 0; i < mock.size(); i++) {
      assertThat(dag.get(i).rule()).as(mock.get(i).name() + " rule").isEqualTo(mock.get(i).pattern());
    }
  }

  @Test
  @DisplayName("every env has the same default value in the DAG and in the mock (None in the DAG means no static default)")
  void sameDefaults() throws IOException {
    List<DagEntry> dag = dagJobEnv();
    List<JobEnv.Entry> mock = JobEnv.defaults().allowed();

    assertThat(dag).hasSameSizeAs(mock);
    for (int i = 0; i < mock.size(); i++) {
      assertThat(dag.get(i).defaultValue()).as(mock.get(i).name() + " default").isEqualTo(mock.get(i).defaultValue());
    }
  }
}
