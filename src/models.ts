// Copyright 2019 Google LLC
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
import {BigQuery} from '@google-cloud/bigquery';
import type {Dataset, Table,  GetModelsResponse, Model} from '@google-cloud/bigquery';
//TODO(coleleah) is this what we want?
const projectId = process.env.GCLOUD_PROJECT
// Import the Google Cloud client library and create a client
const bigquery = new BigQuery();
// TODO(coleleah) change name

async function listModels(datasetId: string){
// List all models
    const dataset = bigquery.dataset(datasetId);

    dataset.getModels().then((data: GetModelsResponse) => {
      const models: Model[] = data[0];
      console.log('Models:');
      models.forEach((model: Model) => console.log(model.metadata));
    });
    // Show us what's going on there - does it have a friendly name, does it have a description, can you change it


}



listModels("nodejs_samples_tests_datasets_f8b28ba7_75ae_4583_adce_d10a97cb5940")


