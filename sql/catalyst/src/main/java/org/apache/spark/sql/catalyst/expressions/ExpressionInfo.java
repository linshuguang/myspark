package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/27.
 */
public class ExpressionInfo {
    private String className;
    private String usage;
    private String name;
    private String extended;
    private String db;
    private String arguments;
    private String examples;
    private String note;
    private String since;

    public String getClassName() {
        return className;
    }

    public String getUsage() {
        return usage;
    }

    public String getName() {
        return name;
    }

    public String getExtended() {
        return extended;
    }

    public String getSince() {
        return since;
    }

    public String getArguments() {
        return arguments;
    }

    public String getExamples() {
        return examples;
    }

    public String getNote() {
        return note;
    }

    public String getDb() {
        return db;
    }

    public ExpressionInfo(
            String className,
            String db,
            String name,
            String usage,
            String arguments,
            String examples,
            String note,
            String since) {
        assert name != null;
        assert arguments != null;
        assert examples != null;
        assert examples.isEmpty() || examples.startsWith("\n    Examples:");
        assert note != null;
        assert since != null;

        this.className = className;
        this.db = db;
        this.name = name;
        this.usage = usage;
        this.arguments = arguments;
        this.examples = examples;
        this.note = note;
        this.since = since;

        // Make the extended description.
        this.extended = arguments + examples;
        if (this.extended.isEmpty()) {
            this.extended = "\n    No example/argument for _FUNC_.\n";
        }
        if (!note.isEmpty()) {
            this.extended += "\n    Note:\n      " + note.trim() + "\n";
        }
        if (!since.isEmpty()) {
            this.extended += "\n    Since: " + since + "\n";
        }
    }

    public ExpressionInfo(String className, String name) {
        this(className, null, name, null, "", "", "", "");
    }

    public ExpressionInfo(String className, String db, String name) {
        this(className, db, name, null, "", "", "", "");
    }

    // This is to keep the original constructor just in case.
    public ExpressionInfo(String className, String db, String name, String usage, String extended) {
        // `arguments` and `examples` are concatenated for the extended description. So, here
        // simply pass the `extended` as `arguments` and an empty string for `examples`.
        this(className, db, name, usage, extended, "", "", "");
    }
}
