package org.apache.sqoop.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.cli.RelatedOptions;
import org.apache.sqoop.cli.ToolOptions;
import org.apache.sqoop.hive.HiveImport;

import java.io.IOException;

public class CreateHiveTable2Tool extends org.apache.sqoop.tool.BaseSqoopTool {

    public static final Log LOG = LogFactory.getLog(
            CreateHiveTable2Tool.class.getName());

    public CreateHiveTable2Tool() {
        super("create-hive-table-2");
    }

    @Override
    public int run(SqoopOptions options) {
        if (!init(options)) {
            return 1;
        }

        try {
            HiveImport hiveImport = new HiveImport(options, manager,
                    options.getConf(), false);
            hiveImport.importTable(options.getTableName(),
                    options.getHiveTableName(), true);
        } catch (IOException ioe) {
            LOG.error("Encountered IOException running create table job: ", ioe);
            rethrowIfRequired(options, ioe);
            return 1;
        } finally {
            destroy(options);
        }

        return 0;
    }

    @Override
    public void configureOptions(ToolOptions toolOptions) {
        toolOptions.addUniqueOptions(getCommonOptions());

        RelatedOptions hiveOpts = getHiveOptions(false);
        hiveOpts.addOption(OptionBuilder.withArgName("statement")
                .hasArg()
                .withDescription("Execute 'statement' in SQL")
                .withLongOpt(SQL_QUERY_ARG)
                .create());
        toolOptions.addUniqueOptions(hiveOpts);

        toolOptions.addUniqueOptions(getOutputFormatOptions());
    }

    @Override
    public void printHelp(ToolOptions toolOptions) {
        super.printHelp(toolOptions);
        System.out.println("");
        System.out.println(
                "At minimum, you must specify --connect and --query");
    }

    @Override
    public void applyOptions(CommandLine in, SqoopOptions out)
            throws SqoopOptions.InvalidOptionsException {

        if (in.hasOption(SQL_QUERY_ARG)) {
            out.setSqlQuery(in.getOptionValue(SQL_QUERY_ARG));
        }

        out.setHiveImport(true);

        applyCommonOptions(in, out);
        applyHiveOptions(in, out);
        applyOutputFormatOptions(in, out);
    }

    @Override
    public void validateOptions(SqoopOptions options)
            throws SqoopOptions.InvalidOptionsException {

        if (hasUnrecognizedArgs(extraArguments)) {
            throw new SqoopOptions.InvalidOptionsException(HELP_STR);
        }

        validateCommonOptions(options);
        validateOutputFormatOptions(options);
        validateHiveOptions(options);

        if (options.getSqlQuery() == null) {
            throw new SqoopOptions.InvalidOptionsException(
                    "--query is required for table definition importing." + HELP_STR);
        }
    }
}
