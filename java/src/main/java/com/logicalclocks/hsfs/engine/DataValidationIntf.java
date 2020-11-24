package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.metadata.Expectation;
import com.logicalclocks.hsfs.metadata.FeatureGroupValidation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public interface DataValidationIntf {

  FeatureGroupValidation runVerification(Dataset<Row> data, List<Expectation> rules);
}
