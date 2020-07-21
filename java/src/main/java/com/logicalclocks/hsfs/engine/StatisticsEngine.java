package com.logicalclocks.hsfs.engine;

import com.amazon.deequ.profiles.ColumnProfilerRunBuilder;
import com.amazon.deequ.profiles.ColumnProfilerRunner;
import com.amazon.deequ.profiles.ColumnProfiles;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;

import java.util.List;

public class StatisticsEngine {
  private static StatisticsEngine INSTANCE = null;

  public static synchronized StatisticsEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new StatisticsEngine();
    }
    return INSTANCE;
  }

  private StatisticsEngine() {}

  public String profile(Dataset<Row> df, List<String> restrictToColumns, boolean correlation, boolean histogram) {
    ColumnProfilerRunBuilder runner =
      new ColumnProfilerRunner().onData(df).withCorrelation(correlation).withHistogram(histogram);
    if (restrictToColumns != null && !restrictToColumns.isEmpty()) {
      runner.restrictToColumns(JavaConverters.asScalaIteratorConverter(restrictToColumns.iterator()).asScala().toSeq());
    }
    ColumnProfiles result = runner.run();
    return ColumnProfiles.toJson(result.profiles().values().toSeq());
  }

  public String profile(Dataset<Row> df, List<String> restrictToColumns) {
    return profile(df, restrictToColumns, true, true);
  }

  public String profile(Dataset<Row> df, boolean correlation, boolean histogram) {
    return profile(df, null, correlation, histogram);
  }

  public String profile(Dataset<Row> df) {
    return profile(df, null, true, true);
  }
}
