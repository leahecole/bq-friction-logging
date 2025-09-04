// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';
// TODO - update import
import {BigQuery} from '@google-cloud/bigquery';
import type * as BigQueryType from '@google-cloud/bigquery';
import {randomUUID} from 'crypto';

const projectId = process.env.GCLOUD_PROJECT
// Import the Google Cloud client library and create a client
// TODO - update client initialization
const bigquery = new BigQuery();




const RESOURCE_PREFIX = 'bq_friction_logging';
const UUID = randomUUID() 
const datasetId = `${RESOURCE_PREFIX}_datasets_${UUID}`.replace(
  /-/gi,
  '_',
);
const modelId = `${RESOURCE_PREFIX}_models_${UUID}`.replace(
  /-/gi,
  '_',
);
 const query = `CREATE OR REPLACE MODEL \`${projectId}.${datasetId}.${modelId}\`
    OPTIONS (
			model_type='linear_reg',
			max_iterations=1,
			learn_rate=0.4,
			learn_rate_strategy='constant'
		) AS (
			SELECT 'a' AS f1, 2.0 AS label
			UNION ALL
			SELECT 'b' AS f1, 3.8 AS label
		)`;

async function createModel() {

    // TODO - update what gets passed to the request
    // Model resources live within datasets, so we need to create a dataset first
    const datasetOptions = {
      location: 'US',
    };
    // TODO - update call and its parameters
    const [dataset] = await bigquery.createDataset(datasetId, datasetOptions);
    console.log(`Dataset ${dataset.id} created.`);
    const queryOptions = {
      query: query,
    };

    // Run query to create a model
    const [job] = await bigquery.createQueryJob(queryOptions);

    // Wait for the query to finish
    await job.getQueryResults();

    console.log(`Model ${modelId} created.`);

  }


async function listModels(datasetId: string){
// List all models
    const dataset = bigquery.dataset(datasetId);

    dataset.getModels().then((data: BigQueryType.GetModelsResponse) => {
      const models: BigQueryType.Model[] = data[0];
      console.log('Models:');
      models.forEach((model: BigQueryType.Model) => console.log(model.metadata));
    });
    // Show us what's going on there - does it have a friendly name, does it have a description, can you change it


}


async function updateModel(modelId: string){
    // TODO - update what gets passed to the request
    // TODO - update call(s) and its parameters
     // Retreive current model metadata
    const model = bigquery.dataset(datasetId).model(modelId);
    const [metadata] = await model.getMetadata();

    // Set new model description
    const description = 'New model description.';
    metadata.description = description;
    const [apiResponse] = await model.setMetadata(metadata);
    const newDescription = apiResponse.description;

    console.log(`${modelId} description: ${newDescription}`);

}

  async function deleteModel(modelId: string) {
    // TODO - update what gets passed to the request
    // Create a reference to the existing dataset
    const dataset = bigquery.dataset(datasetId);
    // TODO - update call and its parameters

    // Create model reference and delete it
    const model = dataset.model(modelId);
    await model.delete();

    console.log(`Model ${modelId} deleted.`);
    // TODO - update call and its parameters
    // Delete the dataset and its contents
    await dataset.delete({force: true});
    console.log(`Dataset ${dataset.id} deleted.`);
  }

// wrap in an async main function so we can make calls in order
async function main(){
    await createModel();
    await listModels(datasetId)
    await updateModel(modelId)
    await deleteModel(modelId)
}
main()




