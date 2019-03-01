package org.apache.spark.sql.catalyst.analysis;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/26.
 */
public class TestRelations {
    public static LocalRelation testRelation = new LocalRelation( new AttributeReference("a", new IntegerType(), true));

    public static LocalRelation testRelation2 = new LocalRelation(
            new AttributeReference("a", new StringType()),
    new AttributeReference("b", new StringType()),
    new AttributeReference("c", new DoubleType()),
    new AttributeReference("d", new DecimalType(10, 2)),
    new AttributeReference("e", new ShortType()));

    public static LocalRelation testRelation3 = new LocalRelation(
            new AttributeReference("e", new ShortType()),
    new AttributeReference("f", new StringType()),
            new AttributeReference("g", new DoubleType()),
            new AttributeReference("h", new DecimalType(10, 2)));

    // This is the same with `testRelation3` but only `h` is incompatible type.
    public static LocalRelation testRelation4 = new LocalRelation(
            new AttributeReference("e", new StringType()),
    new AttributeReference("f", new StringType()),
    new AttributeReference("g", new StringType()),
    new AttributeReference("h", new MapType(new IntegerType(), new IntegerType())));

    public static LocalRelation nestedRelation = new LocalRelation(
            new AttributeReference("top",
                                        new StructType( new StructField("duplicateField", new StringType()),
                                                        new StructField("duplicateField", new StringType()),
                                                        new StructField("differentCase", new StringType()),
                                                        new StructField("differentcase", new StringType())
                                        )));

    public static LocalRelation nestedRelation2 = new LocalRelation(
            new AttributeReference("top", new StructType(
                                            new StructField("aField", new StringType()),
                                            new StructField("bField", new StringType()),
                                            new StructField("cField", new StringType())
                                        )));

    public static LocalRelation listRelation = new LocalRelation(
            new AttributeReference("list", new ArrayType(new IntegerType())));

    public static LocalRelation  mapRelation = new LocalRelation(
            new AttributeReference("map", new MapType(new IntegerType(), new IntegerType())));


}
