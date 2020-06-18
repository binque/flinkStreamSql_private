package org.apache.flink.formats.json;

/*
{
	"type": "object",
	"properties": {
		"city": {
			"type": "string"
		},
		"number": {
			"type": "number"
		},
		"test": {
			"type": "array",
			"items": {
				"type": "number"
			}
		},
		"user": {
			"type": "object",
			"properties": {
				"name": {
					"type": "string"
				},
				"age": {
					"type": "number"
				}
			}
		}
	}
}
 */
public class Test {
    public static void main(String[] args) {

            String s = "{\n" +
                    "\t\"type\": \"object\",\n" +
                    "\t\"properties\": {\n" +
                    "\t\t\"city\": {\n" +
                    "\t\t\t\"type\": \"string\"\n" +
                    "\t\t},\n" +
                    "\t\t\"number\": {\n" +
                    "\t\t\t\"type\": \"number\"\n" +
                    "\t\t},\n" +
                    "\t\t\"test\": {\n" +
                    "\t\t\t\"type\": \"array\",\n" +
                    "\t\t\t\"items\": {\n" +
                    "\t\t\t\t\"type\": \"number\"\n" +
                    "\t\t\t}\n" +
                    "\t\t},\n" +
                    "\t\t\"user\": {\n" +
                    "\t\t\t\"type\": \"object\",\n" +
                    "\t\t\t\"properties\": {\n" +
                    "\t\t\t\t\"name\": {\n" +
                    "\t\t\t\t\t\"type\": \"string\"\n" +
                    "\t\t\t\t},\n" +
                    "\t\t\t\t\"age\": {\n" +
                    "\t\t\t\t\t\"type\": \"number\"\n" +
                    "\t\t\t\t}\n" +
                    "\t\t\t}\n" +
                    "\t\t}\n" +
                    "\t}\n" +
                    "}";
            DTJsonRowDeserializationSchema aa = new DTJsonRowDeserializationSchema(s);
            System.out.println(aa.typeInfo);
    }
}
