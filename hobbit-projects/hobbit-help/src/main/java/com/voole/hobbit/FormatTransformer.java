/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseFieldTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.voole.hobbit.kafka.TopicProtoClassUtils;
import com.voole.hobbit.proto.TerminalPB;

/**
 * @author XuehuiHe
 * @date 2014年7月9日
 */
public class FormatTransformer {

	public static void main(String[] args) throws URISyntaxException,
			IOException {
		List<Descriptor> descriptors = TerminalPB.getDescriptor()
				.getMessageTypes();
		for (Descriptor descriptor : descriptors) {
			String recordName = descriptor.getFullName();
			if (recordName.indexOf("Srv") != -1) {
				continue;
			}
			Schema s = createSchema(descriptor, "com.voole.hobbit.avro.termial");
			File f = createFile(descriptor, "terminal");
			FileWriter fw = new FileWriter(f);
			BufferedWriter writer = new BufferedWriter(fw);
			writer.write(s.toString());
			writer.close();
		}
	}

	public static File createFile(Descriptor descriptor, String packagee)
			throws URISyntaxException, IOException {
		String fileName = getName(descriptor) + ".avro";
		File d = new File(FormatTransformer.class.getClassLoader()
				.getResource("").toURI()).getParentFile().getParentFile();
		File d2 = new File(d.getAbsoluteFile() + "/src/main/avro/" + fileName);
		d2.getParentFile().mkdirs();
		return d2;
	}

	public static String getName(Descriptor descriptor) {
		String clazzName = descriptor.getFullName();
		for (Entry<String, Class<?>> entry : TopicProtoClassUtils.topicMapProtoClass
				.entrySet()) {
			if (entry.getValue().getSimpleName().equals(clazzName)) {
				return entry.getKey();
			}
		}
		return null;
	}

	public static Schema createArraySchema(Descriptor descriptor,
			String packagee) {
		 return SchemaBuilder.unionOf().nullType().and().array()
		 .items(createSchema(descriptor, packagee)).endUnion();
		// return SchemaBuilder.array().items(createSchema(descriptor,
		// packagee));
	}

	public static Schema createSchema(Descriptor descriptor, String packagee) {
		RecordBuilder<Schema> rb = SchemaBuilder.record(
				descriptor.getFullName()).namespace(packagee);
		FieldAssembler<Schema> fa = rb.fields();
		for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
			if (fieldDescriptor.getJavaType() == JavaType.MESSAGE) {
				fa.name("srvs")
						.type(createArraySchema(
								fieldDescriptor.getMessageType(), packagee))
						.noDefault();
			} else {
				BaseFieldTypeBuilder<Schema> bfy = fa
						.name(fieldDescriptor.getName()).type().nullable();
				switch (fieldDescriptor.getJavaType()) {
				case BOOLEAN:
					bfy.booleanType().noDefault();
					break;
				case DOUBLE:
					bfy.doubleType().noDefault();
					break;
				case FLOAT:
					bfy.floatType().noDefault();
					break;
				case INT:
					bfy.intType().noDefault();
					break;
				case LONG:
					bfy.longType().noDefault();
					break;
				case STRING:
					bfy.stringType().noDefault();
					break;
				default:
					System.out.println(fieldDescriptor.getJavaType());
					System.out.println("error");
					break;
				}
			}
		}
		return fa.endRecord();

	}
}
