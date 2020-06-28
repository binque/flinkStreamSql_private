package org.apache.flink.formats.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ContainerNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Objects;

public class DTJsonRowSerializationSchema implements SerializationSchema<Row> {

    private static final long serialVersionUID = -2885556750743978636L;

    /** Type information describing the input type. */
    private final TypeInformation<Row> typeInfo;

    /** Object mapper that is used to create output JSON objects. */
    private final ObjectMapper mapper = new ObjectMapper();

    /** Formatter for RFC 3339-compliant string representation of a time value (with UTC timezone, without milliseconds). */
    private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss'Z'");

    /** Formatter for RFC 3339-compliant string representation of a time value (with UTC timezone). */
    private SimpleDateFormat timeFormatWithMillis = new SimpleDateFormat("HH:mm:ss.SSS'Z'");

    /** Formatter for RFC 3339-compliant string representation of a timestamp value (with UTC timezone). */
    private SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    /** Reusable object node. */
    private transient ObjectNode node;

    public DTJsonRowSerializationSchema(TypeInformation<Row> typeInfo) {
        Preconditions.checkNotNull(typeInfo, "Type information");
        this.typeInfo = typeInfo;
    }

    public DTJsonRowSerializationSchema(String jsonSchema) {
        this(JsonRowSchemaConverter.convert(jsonSchema));
    }

    @Override
    public byte[] serialize(Row row) {
        if (node == null) {
            node = mapper.createObjectNode();
        }

        try {
            convertRow(node, (RowTypeInfo) typeInfo, row);
            return mapper.writeValueAsBytes(node);
        } catch (Throwable t) {
            throw new RuntimeException("Could not serialize row '" + row + "'. " +
                    "Make sure that the schema matches the input.", t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DTJsonRowSerializationSchema that = (DTJsonRowSerializationSchema) o;
        return Objects.equals(typeInfo, that.typeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeInfo);
    }

    // --------------------------------------------------------------------------------------------

    private ObjectNode convertRow(ObjectNode reuse, RowTypeInfo info, Row row) {
        if (reuse == null) {
            reuse = mapper.createObjectNode();
        }
        final String[] fieldNames = info.getFieldNames();
        final TypeInformation<?>[] fieldTypes = info.getFieldTypes();

        // validate the row
        if (row.getArity() != fieldNames.length) {
            throw new IllegalStateException(String.format(
                    "Number of elements in the row '%s' is different from number of field names: %d", row, fieldNames.length));
        }

        for (int i = 0; i < fieldNames.length; i++) {
            final String name = fieldNames[i];

            final JsonNode fieldConverted = convert(reuse, reuse.get(name), fieldTypes[i], row.getField(i));
            reuse.set(name, fieldConverted);
        }

        return reuse;
    }

    private JsonNode convert(ContainerNode<?> container, JsonNode reuse, TypeInformation<?> info, Object object) {
        if (info.equals(Types.VOID) || object == null) {
            return container.nullNode();
        } else if (info.equals(Types.BOOLEAN)) {
            return container.booleanNode((Boolean) object);
        } else if (info.equals(Types.STRING)) {
            return container.textNode((String) object);
        } else if (info.equals(Types.BIG_DEC)) {
            // convert decimal if necessary
            if (object instanceof BigDecimal) {
                return container.numberNode((BigDecimal) object);
            }
            return container.numberNode(BigDecimal.valueOf(((Number) object).doubleValue()));
        } else if (info.equals(Types.BIG_INT)) {
            // convert integer if necessary
            if (object instanceof BigInteger) {
                return container.numberNode((BigInteger) object);
            }
            return container.numberNode(BigInteger.valueOf(((Number) object).longValue()));
        } else if (info.equals(Types.SQL_DATE)) {
            return container.textNode(object.toString());
        } else if (info.equals(Types.SQL_TIME)) {
            final Time time = (Time) object;
            // strip milliseconds if possible
            if (time.getTime() % 1000 > 0) {
                return container.textNode(timeFormatWithMillis.format(time));
            }
            return container.textNode(timeFormat.format(time));
        } else if (info.equals(Types.SQL_TIMESTAMP)) {
            return container.textNode(timestampFormat.format((Timestamp) object));
        } else if (info instanceof RowTypeInfo) {
            if (reuse != null && reuse instanceof ObjectNode) {
                return convertRow((ObjectNode) reuse, (RowTypeInfo) info, (Row) object);
            } else {
                return convertRow(null, (RowTypeInfo) info, (Row) object);
            }
        } else if (info instanceof ObjectArrayTypeInfo) {
            if (reuse != null && reuse instanceof ArrayNode) {
                return convertObjectArray((ArrayNode) reuse, ((ObjectArrayTypeInfo) info).getComponentInfo(), (Object[]) object);
            } else {
                return convertObjectArray(null, ((ObjectArrayTypeInfo) info).getComponentInfo(), (Object[]) object);
            }
        } else if (info instanceof BasicArrayTypeInfo) {
            if (reuse != null && reuse instanceof ArrayNode) {
                return convertObjectArray((ArrayNode) reuse, ((BasicArrayTypeInfo) info).getComponentInfo(), (Object[]) object);
            } else {
                return convertObjectArray(null, ((BasicArrayTypeInfo) info).getComponentInfo(), (Object[]) object);
            }
        } else if (info instanceof PrimitiveArrayTypeInfo && ((PrimitiveArrayTypeInfo) info).getComponentType().equals(Types.BYTE)) {
            return container.binaryNode((byte[]) object);
        } else {
            // for types that were specified without JSON schema
            // e.g. POJOs
            try {
                return mapper.valueToTree(object);
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Unsupported type information '" + info + "' for object: " + object, e);
            }
        }
    }

    private ArrayNode convertObjectArray(ArrayNode reuse, TypeInformation<?> info, Object[] array) {
        if (reuse == null) {
            reuse = mapper.createArrayNode();
        } else {
            reuse.removeAll();
        }

        for (Object object : array) {
            reuse.add(convert(reuse, null, info, object));
        }
        return reuse;
    }
}
