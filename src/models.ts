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
import {BigQueryClient} from '@google-cloud/bigquery';
import type * as BigQueryType from '@google-cloud/bigquery';
import {randomUUID} from 'crypto';
import {setInterval} from 'node:timers/promises';


const projectId = process.env.GCLOUD_PROJECT
// Import the Google Cloud client library and create a client
const bigquery = new BigQueryClient();




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

     // Specify the geographic location where the dataset should reside
    const datasetObject: BigQueryType.protos.google.cloud.bigquery.v2.IDataset = {
      datasetReference: {
        datasetId: datasetId,
      },
      location: 'US',
    };

    // Construct the request object.
    const datasetRequest: BigQueryType.protos.google.cloud.bigquery.v2.IInsertDatasetRequest = {
      projectId: projectId,
      dataset: datasetObject,
    };
    // Create a new dataset
    const [dataset] = await bigquery.insertDataset(datasetRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IDataset,
      BigQueryType.protos.google.cloud.bigquery.v2.IInsertDatasetRequest | undefined,
      {} | undefined,
    ];
    console.log(`Dataset ${dataset.id} created.`);
    // Run query to create a model
    const request = {
      projectId: projectId,
      job: {
        configuration: {
          query: {
            query: query,
            useLegacySql: {value: false},
          },
        },
      },
    };

    // Run query to create a model
    const [jobResponse] = await bigquery.insertJob(request) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IJob,
      BigQueryType.protos.google.cloud.bigquery.v2.IInsertJobRequest | undefined,
      {} | undefined,
    ] ;
    const jobReference = jobResponse.jobReference;

    const getQueryResultsRequest = {
      projectId: projectId,
      jobId: jobReference!.jobId,
      location: jobReference!.location!.value,
    };

    // poll the job status every 3 seconds until complete
    // eslint-disable-next-line
    for await (const t of setInterval(3000)) { // no-unused-vars - this is the syntax for promise based setInterval
      const [resp] = await bigquery.jobClient.getQueryResults(
        getQueryResultsRequest,
      );
      if (resp.errors && resp.errors.length !== 0) {
        throw new Error('Something failed in model creation');
      }
      if (resp.jobComplete && resp.jobComplete.value) {
        break;
      }
    }

    console.log(`Model ${modelId} created.`);

  }


async function listModels(datasetId: string){
  const request = {
      projectId: projectId,
      datasetId: datasetId,
    };

    const [models] = await bigquery.listModels(request) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IModel[],
      BigQueryType.protos.google.cloud.bigquery.v2.IListModelsRequest | null,
      BigQueryType.protos.google.cloud.bigquery.v2.IListModelsResponse,
    ];

    if (models && models.length > 0) {
      console.log('Models:');
      models.forEach(model => console.log(model));
    } else {
      console.log(`No models found in dataset ${datasetId}.`);
    }

}


async function updateModel(modelId: string){
    // known limitation: patchModel must be called in REST fallback mode
    const bigqueryClientREST = new BigQueryClient({}, {opts: {fallback: true}});

  // Retreive current model metadata
   const getRequest = {
      projectId: projectId,
      datasetId: datasetId,
      modelId: modelId,
    };

    const [model] = await bigquery.getModel(getRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IModel,
      BigQueryType.protos.google.cloud.bigquery.v2.IGetModelRequest | undefined,
      {} | undefined,
    ];

    console.log('Model:');
    console.log(model);

    // Set new model description
    const description = 'New model description.';
    const updateRequest = {
      projectId: projectId,
      datasetId: datasetId,
      modelId: modelId,
      model: {
        modelReference: {
          projectId: projectId,
          datasetId: datasetId,
          modelId: modelId,
        },
        description: description,
      },
    };

    const [response] = await bigqueryClientREST.patchModel(updateRequest) as [
      BigQueryType.protos.google.cloud.bigquery.v2.IModel,
      BigQueryType.protos.google.cloud.bigquery.v2.IPatchModelRequest | undefined,
      {} | undefined,
    ];

    console.log(`${modelId} description: ${response.description}`);

}

  async function deleteModel(modelId: string) {
   const request = {
      projectId: projectId,
      datasetId: datasetId,
      modelId: modelId,
    };

    await bigquery.deleteModel(request);

    console.log(`Model ${modelId} deleted.`);
    // Delete the dataset and its contents
    const deleteRequest: BigQueryType.protos.google.cloud.bigquery.v2.IDeleteDatasetRequest = {
      projectId: projectId,
      datasetId: datasetId,
      deleteContents: true, // Set to true to delete all tables in the dataset
    };
    await bigquery.deleteDataset(deleteRequest);
      console.log(`Dataset ${datasetId} deleted.`);
  }

// wrap in an async main function so we can make calls in order
async function main(){
    await createModel();
    await listModels(datasetId)
    await updateModel(modelId)
    await deleteModel(modelId)
}
main()



