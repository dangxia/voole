package com.voole.avro.usage;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.tool.SpecificCompilerTool;

public class CompileSchema {
	static String avroJavaFileTarget;
	static String resoucesPath;
	static {
		String projectPath = new File(CompileSchema.class.getClassLoader()
				.getResource("").getPath()).getParentFile().getParent();

		avroJavaFileTarget = projectPath + File.separator + "src"
				+ File.separator + "main" + File.separator + "java";

		resoucesPath = projectPath + File.separator + "src" + File.separator
				+ "main" + File.separator + "resources";

	}

	public static void main(String[] args2) throws Exception {
		SpecificCompilerTool tool = new SpecificCompilerTool();
		List<String> args = new ArrayList<String>();
		args.add("schema");
		args.add(getAvroFilePath("avro/avro_model_version1.avro"));
		args.add(getAvroFilePath("avro/avro_model_version2.avro"));
//		args.add(getAvroFilePath("avro/*.avro"));
		args.add(avroJavaFileTarget);

		tool.run(System.in, System.out, System.err, args);
	}

	public static String getAvroFilePath(String path) {
		return resoucesPath + File.separator + path;
	}
}
