package org.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import software.amazon.awssdk.services.redshiftdata.model.ColumnMetadata;
import software.amazon.awssdk.services.redshiftdata.model.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryResult {
    private final List<Map<String, String>> table;

    public QueryResult(List<ColumnMetadata> columnMetadata, List<List<Field>> rows) {
        List<Map<String, String>> result = new ArrayList<>();

        // Process rows
        for (List<Field> row : rows) {
            Map<String, String> rowMap = new HashMap<>();
            for (int i = 0; i < row.size(); i++) {
                String columnName = columnMetadata.get(i).name();
                String fieldValue = getFieldValue(row.get(i));
                rowMap.put(columnName, fieldValue);
            }
            result.add(rowMap);
        }
        this.table = result;
    }

    private static String getFieldValue(Field field) {
        if (field == null) {
            return "NULL";
        } else if (field.stringValue() != null) {
            return field.stringValue();
        } else if (field.booleanValue() != null) {
            return field.booleanValue().toString();
        } else if (field.longValue() != null) {
            return field.longValue().toString();
        }
        return "";
    }

    @Override
    public String toString() {
        return table.toString();
    }

    public String toJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(table);
    }


}
