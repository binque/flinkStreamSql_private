package com.cj.flink.sql.sink.kafka.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.csv.CsvRowSchemaConverter;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.types.CRowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ContainerNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import com.cj.flink.sql.enums.EUpdateMode;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;

public class CsvCRowSerializationSchema implements SerializationSchema<CRow> {

    private static final long serialVersionUID = 2098447220136965L;

    /** Type information describing the input CSV data. */
    private final RowTypeInfo typeInfo;

    /** Runtime instance that performs the actual work. */
    private final RuntimeConverter runtimeConverter;

    /** CsvMapper used to write {@link JsonNode} into bytes. */
    private final CsvMapper csvMapper;

    /** Schema describing the input CSV data. */
    private CsvSchema csvSchema;

    /** Object writer used to write rows. It is configured by {@link CsvSchema}. */
    private ObjectWriter objectWriter;

    /** Reusable object node. */
    private transient ObjectNode root;

    private String updateMode;

    private String retractKey = "retract";

    private CsvCRowSerializationSchema(
            RowTypeInfo typeInfo,
            CsvSchema csvSchema,
            String updateMode) {
        this.typeInfo = typeInfo;
        this.runtimeConverter = createRowRuntimeConverter(typeInfo, true);
        this.csvMapper = new CsvMapper();
        this.csvSchema = csvSchema;
        this.updateMode = updateMode;
        this.objectWriter = csvMapper.writer(csvSchema);
    }

    @PublicEvolving
    public static class Builder {

        private final RowTypeInfo typeInfo;
        private CsvSchema csvSchema;
        private String updateMode;

        /**
         * Creates a {@link CsvRowSerializationSchema} expecting the given {@link TypeInformation}.
         *
         * @param typeInfo type information used to create schema.
         */
        public Builder(TypeInformation<CRow> typeInfo) {
            Preconditions.checkNotNull(typeInfo, "Type information must not be null.");

            if (!(typeInfo instanceof CRowTypeInfo)) {
                throw new IllegalArgumentException("Row type information expected.");
            }
            RowTypeInfo rowTypeInfo = ((CRowTypeInfo) typeInfo).rowType();
            this.typeInfo = rowTypeInfo;
            this.csvSchema = CsvRowSchemaConverter.convert(rowTypeInfo);
        }

        public Builder setFieldDelimiter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(c).build();
            return this;
        }

        public Builder setLineDelimiter(String delimiter) {
            Preconditions.checkNotNull(delimiter, "Delimiter must not be null.");
            if (!("\n".equals(delimiter)) && !("\r".equals(delimiter)) && !("\r\n".equals(delimiter))) {
                throw new IllegalArgumentException(
                        "Unsupported new line delimiter. Only \\n, \\r, or \\r\\n are supported.");
            }
            this.csvSchema = this.csvSchema.rebuild().setLineSeparator(delimiter).build();
            return this;
        }

        public Builder setArrayElementDelimiter(String delimiter) {
            Preconditions.checkNotNull(delimiter, "Delimiter must not be null.");
            this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
            return this;
        }

        public Builder setQuoteCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
            return this;
        }

        public Builder setEscapeCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
            return this;
        }

        public Builder setNullLiteral(String s) {
            this.csvSchema = this.csvSchema.rebuild().setNullValue(s).build();
            return this;
        }

        public Builder setUpdateMode(String updateMode) {
            this.updateMode = updateMode;
            return this;
        }

        public CsvCRowSerializationSchema build() {
            return new CsvCRowSerializationSchema(
                    typeInfo,
                    csvSchema,
                    updateMode);
        }
    }

    @Override
    public byte[] serialize(CRow crow) {
        Row row = crow.row();
        boolean change = crow.change();
        if (root == null) {
            root = csvMapper.createObjectNode();
        }
        try {
            runtimeConverter.convert(csvMapper, root, row);
            if (StringUtils.equalsIgnoreCase(updateMode, EUpdateMode.UPSERT.name())) {
                fillRetractField(row, change);
            }

            return objectWriter.writeValueAsBytes(root);
        } catch (Throwable t) {
            throw new RuntimeException("Could not serialize row '" + row + "'.", t);
        }
    }

    protected void fillRetractField(Row row, boolean change) {
        root.put(retractKey, change);
        CsvSchema.Builder newBuilder = new CsvSchema.Builder(csvSchema);

        CsvSchema.Column retractColumn = new CsvSchema.Column(row.getArity(), retractKey, CsvSchema.ColumnType.BOOLEAN);
        newBuilder.addColumn(retractColumn);
        csvSchema = newBuilder.build();

        this.objectWriter = csvMapper.writer(csvSchema);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        if (this == o) {
            return true;
        }
        final CsvCRowSerializationSchema that = (CsvCRowSerializationSchema) o;
        final CsvSchema otherSchema = that.csvSchema;

        return typeInfo.equals(that.typeInfo) &&
                csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator() &&
                Arrays.equals(csvSchema.getLineSeparator(), otherSchema.getLineSeparator()) &&
                csvSchema.getArrayElementSeparator().equals(otherSchema.getArrayElementSeparator()) &&
                csvSchema.getQuoteChar() == otherSchema.getQuoteChar() &&
                csvSchema.getEscapeChar() == otherSchema.getEscapeChar() &&
                Arrays.equals(csvSchema.getNullValue(), otherSchema.getNullValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                typeInfo,
                csvSchema.getColumnSeparator(),
                csvSchema.getLineSeparator(),
                csvSchema.getArrayElementSeparator(),
                csvSchema.getQuoteChar(),
                csvSchema.getEscapeChar(),
                csvSchema.getNullValue());
    }

    //--------------------------------------------------------------------------------------------------------------------

    private interface RuntimeConverter extends Serializable {
        JsonNode convert(CsvMapper csvMapper, ContainerNode<?> container, Object obj);
    }

    private static RuntimeConverter createRowRuntimeConverter(RowTypeInfo rowTypeInfo, boolean isTopLevel) {
        final TypeInformation[] fieldTypes = rowTypeInfo.getFieldTypes();
        final String[] fieldNames = rowTypeInfo.getFieldNames();
        final RuntimeConverter[] fieldConverters = createFieldRuntimeConverters(fieldTypes);

        return assembleRowRuntimeConverter(isTopLevel, fieldNames, fieldConverters);
    }

    private static RuntimeConverter assembleRowRuntimeConverter(
            boolean isTopLevel,
            String[] fieldNames,
            RuntimeConverter[] fieldConverters) {
        final int rowArity = fieldNames.length;
        // top level reuses the object node container
        if (isTopLevel) {
            return (csvMapper, container, obj) -> {
                final Row row = (Row) obj;

                validateArity(rowArity, row.getArity());

                final ObjectNode objectNode = (ObjectNode) container;
                for (int i = 0; i < rowArity; i++) {
                    objectNode.set(
                            fieldNames[i],
                            fieldConverters[i].convert(csvMapper, container, row.getField(i)));
                }
                return objectNode;
            };
        } else {
            return (csvMapper, container, obj) -> {
                final Row row = (Row) obj;

                validateArity(rowArity, row.getArity());

                final ArrayNode arrayNode = csvMapper.createArrayNode();
                for (int i = 0; i < rowArity; i++) {
                    arrayNode.add(fieldConverters[i].convert(csvMapper, arrayNode, row.getField(i)));
                }
                return arrayNode;
            };
        }
    }

    private static RuntimeConverter[] createFieldRuntimeConverters(TypeInformation<?>[] fieldTypes) {
        final RuntimeConverter[] fieldConverters = new RuntimeConverter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldConverters[i] = createNullableRuntimeConverter(fieldTypes[i]);
        }
        return fieldConverters;
    }

    private static RuntimeConverter createNullableRuntimeConverter(TypeInformation<?> info) {
        final RuntimeConverter valueConverter = createRuntimeConverter(info);
        return (csvMapper, container, obj) -> {
            if (obj == null) {
                return container.nullNode();
            }
            return valueConverter.convert(csvMapper, container, obj);
        };
    }

    private static RuntimeConverter createRuntimeConverter(TypeInformation<?> info) {
        if (info.equals(Types.VOID)) {
            return (csvMapper, container, obj) -> container.nullNode();
        } else if (info.equals(Types.STRING)) {
            return (csvMapper, container, obj) -> container.textNode((String) obj);
        } else if (info.equals(Types.BOOLEAN)) {
            return (csvMapper, container, obj) -> container.booleanNode((Boolean) obj);
        } else if (info.equals(Types.BYTE)) {
            return (csvMapper, container, obj) -> container.numberNode((Byte) obj);
        } else if (info.equals(Types.SHORT)) {
            return (csvMapper, container, obj) -> container.numberNode((Short) obj);
        } else if (info.equals(Types.INT)) {
            return (csvMapper, container, obj) -> container.numberNode((Integer) obj);
        } else if (info.equals(Types.LONG)) {
            return (csvMapper, container, obj) -> container.numberNode((Long) obj);
        } else if (info.equals(Types.FLOAT)) {
            return (csvMapper, container, obj) -> container.numberNode((Float) obj);
        } else if (info.equals(Types.DOUBLE)) {
            return (csvMapper, container, obj) -> container.numberNode((Double) obj);
        } else if (info.equals(Types.BIG_DEC)) {
            return (csvMapper, container, obj) -> container.numberNode((BigDecimal) obj);
        } else if (info.equals(Types.BIG_INT)) {
            return (csvMapper, container, obj) -> container.numberNode((BigInteger) obj);
        } else if (info.equals(Types.SQL_DATE)) {
            return (csvMapper, container, obj) -> container.textNode(obj.toString());
        } else if (info.equals(Types.SQL_TIME)) {
            return (csvMapper, container, obj) -> container.textNode(obj.toString());
        } else if (info.equals(Types.SQL_TIMESTAMP)) {
            return (csvMapper, container, obj) -> container.textNode(obj.toString());
        } else if (info instanceof RowTypeInfo){
            return createRowRuntimeConverter((RowTypeInfo) info, false);
        } else if (info instanceof BasicArrayTypeInfo) {
            return createObjectArrayRuntimeConverter(((BasicArrayTypeInfo) info).getComponentInfo());
        } else if (info instanceof ObjectArrayTypeInfo) {
            return createObjectArrayRuntimeConverter(((ObjectArrayTypeInfo) info).getComponentInfo());
        } else if (info instanceof PrimitiveArrayTypeInfo &&
                ((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
            return createByteArrayRuntimeConverter();
        }
        else {
            throw new RuntimeException("Unsupported type information '" + info + "'.");
        }
    }

    private static RuntimeConverter createObjectArrayRuntimeConverter(TypeInformation<?> elementType) {
        final RuntimeConverter elementConverter = createNullableRuntimeConverter(elementType);
        return (csvMapper, container, obj) -> {
            final Object[] array = (Object[]) obj;
            final ArrayNode arrayNode = csvMapper.createArrayNode();
            for (Object element : array) {
                arrayNode.add(elementConverter.convert(csvMapper, arrayNode, element));
            }
            return arrayNode;
        };
    }

    private static RuntimeConverter createByteArrayRuntimeConverter() {
        return (csvMapper, container, obj) -> container.binaryNode((byte[]) obj);
    }

    private static void validateArity(int expected, int actual) {
        if (expected != actual) {
            throw new RuntimeException("Row length mismatch. " + expected +
                    " fields expected but was " + actual + ".");
        }
    }
}
