/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.examples.Ambrite.ExtractNumsLowerFn;
import org.apache.beam.examples.Ambrite.ProcessNames;
import org.apache.beam.examples.Ambrite.User;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of WordCount. */
@RunWith(JUnit4.class)
public class AmbriteTest {

	/** Example test that tests a specific {@link DoFn}. */
	@Test
	public void testExtractNumsLowerFn() throws Exception {
		List<String> words = Arrays.asList("1,Shell1y,p@ss55w0ord", "1,1", "1,Commas,C0mmas", "foo,f00", "bar,BaR",
				",,", "", ",");
		PCollection<String> output = p.apply(Create.of(words).withCoder(StringUtf8Coder.of()))
				.apply(ParDo.of(new ExtractNumsLowerFn()));
		PAssert.that(output).containsInAnyOrder("shelly,p@ssword", ",", "commas,cmmas", "foo,f", "bar,bar", ",", ",",
				",");
		p.run().waitUntilFinish();
	}

	static final String[] USERS_ARRAY = new String[] { "1,Mabelle,AAKVwRLMqGm", "2,Nowell,NoXkciK6e",
			"3,Wil1ie,RhlmaO0", ",", ",,", "" };

	static final List<String> USERS = Arrays.asList(USERS_ARRAY);

	static final String[] PROCESSED_ARRAY = new String[] { "mabelle,aakvwrlmqgm", "wilie,rhlmao", "nowell,noxkcike",
			"," };

	@Rule
	public TestPipeline p = TestPipeline.create();

	/**
	 * Example test that tests a PTransform by using an in-memory input and
	 * inspecting the output.
	 */
	@Test
	@Category(ValidatesRunner.class)
	public void testProcessNames() throws Exception {
		PCollection<String> input = p.apply(Create.of(USERS).withCoder(StringUtf8Coder.of()));

		PCollection<String> output = input.apply(new ProcessNames());

		PAssert.that(output).containsInAnyOrder(PROCESSED_ARRAY);
		p.run().waitUntilFinish();
	}
}
