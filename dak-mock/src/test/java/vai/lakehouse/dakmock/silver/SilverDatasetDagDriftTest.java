package vai.lakehouse.dakmock.silver;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link SilverDataset} and {@code DATASETS} in airflow/dags/silver_ingest_vtt.py and airflow/dags/silver_ingest_vtnet.py describe the same
 * datasets: the mock routes a request to the DAG of its dataset and writes the approved settings into the Airflow Variable that the DAG
 * factory (airflow/dags/silver_ingest_factory.py) reads for that dataset. The test reads the DAG files themselves, so changing one side and
 * forgetting the other turns it red. It only checks the DAG files in the repository, not the DAGs deployed on Airflow.
 */
class SilverDatasetDagDriftTest {

  private static final Path DAGS = Path.of("../airflow/dags");
  private static final Pattern ENTRY = Pattern.compile("\"([a-z0-9_-]+)\":\\s*(\\{)\\s*\"mode\":\\s*\"([a-z-]+)\"");

  /** One dataset as written in a DAG file. */
  private record DagDataset(String id, String mode, boolean hasHourLayout) {}

  private static String read(String file) throws IOException {
    return Files.readString(DAGS.resolve(file), StandardCharsets.UTF_8);
  }

  private static String withoutWhitespace(String text) {
    return text.replaceAll("\\s+", "");
  }

  private static String dagFile(SilverSource source) {
    return "silver_ingest_" + source.name().toLowerCase(Locale.ROOT) + ".py";
  }

  /** The text of the entry whose {@code {} is at {@code open}, up to its matching {@code }} (an entry may hold a nested {@code resources} dict). */
  private static String entryOf(String block, int open) {
    int depth = 0;
    for (int i = open; i < block.length(); i++) {
      char c = block.charAt(i);
      if (c == '{') {
        depth++;
      } else if (c == '}' && --depth == 0) {
        return block.substring(open, i + 1);
      }
    }
    throw new IllegalStateException("unbalanced braces in DATASETS");
  }

  private static List<DagDataset> dagDatasets(SilverSource source) throws IOException {
    String text = read(dagFile(source));
    int start = text.indexOf("DATASETS = {");
    int end = text.indexOf("\n}\n", start);
    assertThat(start).as("DATASETS block exists in " + dagFile(source)).isGreaterThanOrEqualTo(0);
    assertThat(end).isGreaterThan(start);
    String block = text.substring(start, end);
    Matcher m = ENTRY.matcher(block);
    List<DagDataset> datasets = new ArrayList<>();
    while (m.find()) {
      datasets.add(new DagDataset(m.group(1), m.group(3), entryOf(block, m.start(2)).contains("\"hour_glob\"")));
    }
    return datasets;
  }

  @Test
  @DisplayName("each DAG file lists exactly the datasets of its source in the mock, in the same order")
  void sameDatasets() throws IOException {
    for (SilverSource source : SilverSource.values()) {
      assertThat(dagDatasets(source)).as(dagFile(source)).extracting(DagDataset::id)
          .containsExactlyElementsOf(SilverDataset.of(source).stream().map(SilverDataset::id).toList());
    }
  }

  @Test
  @DisplayName("every dataset has the same mode in the DAG file and in the mock")
  void sameModes() throws IOException {
    for (SilverSource source : SilverSource.values()) {
      List<DagDataset> dag = dagDatasets(source);
      List<SilverDataset> mock = SilverDataset.of(source);

      assertThat(dag).as(dagFile(source)).hasSameSizeAs(mock);
      for (int i = 0; i < mock.size(); i++) {
        assertThat(dag.get(i).mode()).as(mock.get(i).id() + " mode").isEqualTo(mock.get(i).mode());
      }
    }
  }

  @Test
  @DisplayName("a dataset can run hourly in the mock exactly when its DAG file declares an hour_glob")
  void sameHourLayouts() throws IOException {
    for (SilverSource source : SilverSource.values()) {
      List<DagDataset> dag = dagDatasets(source);
      List<SilverDataset> mock = SilverDataset.of(source);

      assertThat(dag).as(dagFile(source)).hasSameSizeAs(mock);
      for (int i = 0; i < mock.size(); i++) {
        assertThat(dag.get(i).hasHourLayout()).as(mock.get(i).id() + " hour layout").isEqualTo(mock.get(i).supportsHours());
      }
    }
  }

  @Test
  @DisplayName("the DAG id and the Airflow Variable of a dataset are silver_ingest_<source>_<dataset>, as the mock writes them")
  void sameDagIdsAndVariableKeys() throws IOException {
    String factory = read("silver_ingest_factory.py");
    assertThat(factory).contains("return f\"silver_ingest_{prefix}_{dataset.replace('-', '_')}\"");
    assertThat(factory).contains("Variable.get(dag_id"); // the Variable has the name of the DAG
    for (SilverSource source : SilverSource.values()) {
      // A formatter (black) may wrap the call over several lines: compare without whitespace.
      assertThat(withoutWhitespace(read(dagFile(source))))
          .contains("factory.register(globals(),\"" + source.name().toLowerCase(Locale.ROOT) + "\",DATASETS");
    }
    for (SilverDataset dataset : SilverDataset.values()) {
      assertThat(dataset.dagId()).isEqualTo(
          "silver_ingest_" + dataset.source().name().toLowerCase(Locale.ROOT) + "_" + dataset.id().replace('-', '_'));
      assertThat(dataset.variableKey()).isEqualTo(dataset.dagId());
    }
  }

  @Test
  @DisplayName("the keys of the settings that the mock writes are the ones the DAG files accept")
  void sameScheduleKeys() throws IOException {
    for (SilverSource source : SilverSource.values()) {
      String dag = read(dagFile(source));
      for (String key : List.of("ticket", "table", "cron", "outputMode")) {
        assertThat(dag).as(dagFile(source) + " schedule key " + key).contains("\"" + key + "\"");
      }
    }
    String vtnet = read("silver_ingest_vtnet.py");
    for (String key : List.of("encryptColumns", "keyField")) {
      assertThat(vtnet).as("silver_ingest_vtnet.py schedule key " + key).contains("\"" + key + "\"");
    }
  }
}
