
public class PrintSchemaApplication {
    public static void main(String[] args) {
//        InferCSVSchema parser = new InferCSVSchema();
//        parser.printSchema();

        DefineCSVSchema schema = new DefineCSVSchema();
        schema.printDefinedSchema();

    }
}
