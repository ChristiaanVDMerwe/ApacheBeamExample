/**
 * Execute with:
 * mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.Ambrite \
     -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner
 */
package org.apache.beam.examples;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Ambrite {

	/**
	 * User class to which input is mapped with accompanying getter and setters as
	 * well as an overide equals and toString function.
	 */
	public static class User {
		private String name;
		private String password;

		public User(String[] in) {
			if (in.length == 0 || in.length == 1) {
				this.name = "";
				this.password = "";
			} else if (in.length == 2) {
				this.name = in[0].replaceAll("\\d", "").toLowerCase();
				this.password = in[1].replaceAll("\\d", "").toLowerCase();
			} else {
				this.name = in[1].replaceAll("\\d", "").toLowerCase();
				this.password = in[2].replaceAll("\\d", "").toLowerCase();
			}
		}

		@Override
		public String toString() {
			return this.name + "," + this.password;
		}

		@Override
		public boolean equals(Object o) {
			User other = (User) o;
			if (this.name != other.getName()) {
				return false;
			}
			if (this.password != other.getPassword()) {
				return false;
			}
			return true;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setPassword(String pass) {
			this.password = pass;
		}

		public String getName() {
			return this.name;
		}

		public String getPassword() {
			return this.password;
		}
	}

	/**
	 * Removes all numbers and converts to lowercase.
	 */
	static class ExtractNumsLowerFn extends DoFn<String, String> {
		@ProcessElement
		public void processElement(@Element String element, OutputReceiver<String> receiver) {
			String[] words = element.split(",");
			User user = new User(words);
			receiver.output(user.toString());
		}
	}

	public static class ProcessNames extends PTransform<PCollection<String>, PCollection<String>> {
		@Override
		public PCollection<String> expand(PCollection<String> lines) {

			PCollection<String> words = lines.apply(ParDo.of(new ExtractNumsLowerFn()));

			PCollection<String> uniquewords = words.apply(Distinct.<String>create());

			return uniquewords;
		}
	}

	/**
	 * Creates the pipline and processes the input
	 */
	static void runAmbrite() {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		p.apply(TextIO.read().from("input10.csv")).apply(new ProcessNames())
				.apply(TextIO.write().to("output").withSuffix(".csv"));
		p.run().waitUntilFinish();
	}

	public static void main(String[] args) {
		runAmbrite();
	}
}