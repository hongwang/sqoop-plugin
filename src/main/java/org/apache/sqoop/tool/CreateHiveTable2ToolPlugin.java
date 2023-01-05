package org.apache.sqoop.tool;

import java.util.Collections;
import java.util.List;

public class CreateHiveTable2ToolPlugin extends ToolPlugin {
    @Override
    public List<ToolDesc> getTools() {
        return Collections
                .singletonList(new ToolDesc("create-hive-table-2", CreateHiveTable2Tool.class, "Import a query statement into Hive"));
    }
}
