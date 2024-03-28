# Helper script for Anomalo <> Datahub integration script

# Section 1: Base attributes and functions
import time
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    DatasetAssertionInfo,
    DatasetAssertionScope,
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    EditableDatasetPropertiesClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    ChangeTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import PartitionSpec
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProfile
from datahub.metadata.schema_classes import DatasetProfileClass
import anomalo
import uuid
import time
from typing import Optional


def datasetUrn(platform: str, tbl: str) -> str:
    return builder.make_dataset_urn(platform, tbl)
def emitAssertionResult(assertionResult: AssertionRunEvent, emitter: DatahubRestEmitter) -> None:
    dataset_assertionRunEvent_mcp = MetadataChangeProposalWrapper(
        entityType="assertion",
        changeType=ChangeType.UPSERT,
        entityUrn=assertionResult.assertionUrn,
        aspectName="assertionRunEvent",
        aspect=assertionResult,
    )
    # Emit BatchAssertion Result! (timseries aspect)
    emitter.emit_mcp(dataset_assertionRunEvent_mcp)

def assertion_pass_fail(test, table, res, url, msg, platform, emitter):
    asserteeUrn=datasetUrn(platform, table)
    assertion_res = AssertionRunEvent(
        timestampMillis=int(time.time() * 1000),
        # assertionUrn=builder.make_assertion_urn(test),
        asserteeUrn=asserteeUrn,
        assertionUrn=builder.make_assertion_urn(
            builder.datahub_guid(
                {
                    "platform": platform,
                    "nativeType": test,
                    "dataset": asserteeUrn,
                }
            )
        ),  
        runId=uuid.uuid4().hex, # what is this for?
        status=AssertionRunStatus.COMPLETE,
        result=AssertionResult(type=AssertionResultType.SUCCESS, externalUrl=url, nativeResults={"URL":url,"Msg":msg}) if (res == "pass") else AssertionResult(type=AssertionResultType.FAILURE, externalUrl=url, nativeResults={"URL":url,"Msg":msg})
    )
    emitAssertionResult(assertion_res, emitter)

def add_link(institutional_memory_element, link_to_add, table, platform, graph):
    dataset_urn = make_dataset_urn(platform=platform, name=table)
    current_institutional_memory = graph.get_aspect_v2(
        entity_urn=dataset_urn,
        aspect="institutionalMemory",
        aspect_type=InstitutionalMemoryClass,
    )
    need_write = False
    if current_institutional_memory:
        if link_to_add not in [x.url for x in current_institutional_memory.elements]:
            current_institutional_memory.elements.append(institutional_memory_element)
            need_write = True
    else:
        # create a brand new institutional memory aspect
        current_institutional_memory = InstitutionalMemoryClass(
            elements=[institutional_memory_element]
        )
        need_write = True
    if need_write:
        event = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_urn,
            aspectName="institutionalMemory",
            aspect=current_institutional_memory,
        )
        graph.emit(event)

    current_tags: Optional[GlobalTagsClass] = graph.get_aspect_v2(
        entity_urn=dataset_urn,
        aspect="globalTags",
        aspect_type=GlobalTagsClass,
    )
    tag_to_add = make_tag_urn("Monitored_By_Anomalo")
    tag_association_to_add = TagAssociationClass(tag=tag_to_add)
    need_write = False
    if current_tags:
        if tag_to_add not in [x.tag for x in current_tags.tags]:
            # tags exist, but this tag is not present in the current tags
            current_tags.tags.append(TagAssociationClass(tag_to_add))
            need_write = True
    else:
        # create a brand new tags aspect
        current_tags = GlobalTagsClass(tags=[tag_association_to_add])
        need_write = True

    if need_write:
        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_urn,
            aspectName="globalTags",
            aspect=current_tags,
        )
        graph.emit(event)

class EmitMCP:
    def __init__(self, nativeType, tbl, emitter, assertion_dataPlatformInstance, platform):
        self.nativeType = nativeType
        self.tbl = tbl
        self.emitter = emitter
        self.assertion_dataPlatformInstance = assertion_dataPlatformInstance
        self.platform = platform
        self.datasetUrn = datasetUrn(platform, tbl)

    def create_assertion_info(self):
        assertion_info = AssertionInfo(
            type=AssertionType.DATASET,
            datasetAssertion=DatasetAssertionInfo(
                scope=DatasetAssertionScope.DATASET_SCHEMA,
                operator=AssertionStdOperator._NATIVE_,
                nativeType=self.nativeType,
                aggregation=AssertionStdAggregation.COLUMNS,
                dataset=self.datasetUrn,
                ),
            )
        self.assertion_info = assertion_info
        return assertion_info
    
    def create_mcp(self, aspectName):
        if aspectName == "assertionInfo":
            aspect = self.assertion_info
        elif aspectName == "dataPlatformInstance":
            aspect = self.assertion_dataPlatformInstance
        else: 
            raise
            
        mcp = MetadataChangeProposalWrapper(
            entityType="assertion",
            changeType=ChangeType.UPSERT,
            entityUrn=builder.make_assertion_urn(
                builder.datahub_guid(
                    {
                        "platform": self.platform,
                        "nativeType": self.nativeType,
                        "dataset": self.datasetUrn,
                    }
                )
            ),
            aspectName=aspectName,
            aspect=aspect,
        )
        
        self.mcp = mcp
        return mcp
        
    def emit_mcp(self):
        self.emitter.emit_mcp(self.mcp)
        
    def emit(self):
        self.emitter.emit(self.mcp)

    def run(self):
        self.create_assertion_info()
        self.create_mcp(aspectName="assertionInfo")
        self.emit_mcp()
        self.create_mcp(aspectName="dataPlatformInstance")

# Section 2: Function wrapper to push Anomalo data into Datahub
# This function will be imported into the Anomalo <> Datahub integration DAG
def anomalo_to_datahub(api_client: anomalo.Client, start_date: str, end_date: str) -> None:
    '''
    Main function of Anomalo <> Datahub integration script that pushes Anomalo data into Datahub
    Args:
        api_client: Authenticated Anomalo API Client (will not work if no client is passed)
        start_date: Start date of Anomalo check interval to be inserted into Datahub (in "%Y-%m-%d" format)
        end_date: End date of Anomalo check interval to be inserted into Datahub (in "%Y-%m-%d" format)
    '''
    # Please replace all variables below with those unique to your instance
    # TODO: Retrieve below IP address using AWS Secrets Manager
    gms_server = 'http://34.210.197.39:8080' # your gms server
    # anomalo_url = "https://CUSTOMER.ANOMALO.COM/dashboard/tables/"
    anomalo_url = "https://app.anomalo.com/dashboard/home/search"

    # List out platforms (i.e. data warehouses) that the Sandbox data pipeline interacts with
    # Supported platforms: https://datahubproject.io/docs/generated/metamodel/entities/dataplatform
    platforms = ["bigquery", "snowflake"]
    graph = DataHubGraph(config=DatahubClientConfig(server=gms_server))

    # Create an emitter to the GMS REST API.
    emitter = DatahubRestEmitter(gms_server = gms_server)
    # Construct assertion platform objects (will access each as needed)
    assertion_dataPlatformInstances = {
        platform: DataPlatformInstance(
            platform=builder.make_data_platform_urn(platform)
        ) for platform in platforms
    }
    '''
    The actual full name of a table can't be retrieved via Anomalo API.
    We have to map the warehouse ID to the catalog/database name.
    For simplicity, I'm just doing 2 warehouses. You can obtain via /admin/main/warehouse/
    '''
    #warehouse_mapping = {3566:"DBX_Anomalo", 1234:"Test_DW"} # These are your warehouses in Anomalo from /admin/main/warehouse/
    # from: https://app.anomalo.com/api/public/v1/list_warehouses rather than the above
    # {"warehouses": [{"id": 4633, "name": "Snowflake", "warehouse_type": "snowflake"}, {"id": 4919, "name": "qbiz-snowflake-sandbox-pipeline", "warehouse_type": "snowflake"}, {"id": 5611, "name": "qbiz-bigquery-sbx-pipeline", "warehouse_type": "bigquery"}, {"id": 5644, "name": "qbiz-bigquery-sandbox-pipeline", "warehouse_type": "bigquery"}]}

    warehouse_mapping = {4633: 'Snowflake', 4919: 'qbiz-snowflake-sandbox-pipeline', 5611: 'qbiz-bigquery-sbx-pipeline', 5644: 'qbiz-bigquery-sandbox-pipeline'}
    configured_tables = api_client.configured_tables()

    for configured_table in configured_tables:
        anomalo_table_name = configured_table['table']['full_name']
        anomalo_warehouse_id = configured_table['table']['warehouse_id']
        anomalo_warehouse_name = warehouse_mapping[anomalo_warehouse_id]
        tbl = f"{anomalo_warehouse_name}.{anomalo_table_name}"
        
        try:
            anomalo_warehouse_name = warehouse_mapping[anomalo_warehouse_id]
            print("Pushing results to " + tbl)
        except Exception as e:
            print("Error: WH " + str(anomalo_warehouse_id) + " DNE. Skipping " + anomalo_table_name)
            continue

        # Below dhtbl variable must equal the fully qualified table path you see in Datahub's table navigation on top
        # For example, everything after /browse/dataset/prod/DW_NAME
        # NOTE: Not all datasets will show up when using above method ^ instead browse by platform until the table with a link to the platform is found
        # The missing piece is the "database" part of full table path (or "project" in BigQuery); the anomalo_table_name will have the schema + table name
        database_mapping = {'qbiz-snowflake-sandbox-pipeline': 'SANDBOX_DATA_PIPELINE', 'qbiz-bigquery-sandbox-pipeline': 'sandbox-data-pipeline'}
        database = database_mapping[anomalo_warehouse_name]
        dhtbl = f"{database}.{anomalo_table_name}"

        table_id = configured_table['table']['id']
        # Get job id of most recent check run within specified interval
        latest_job_id = api_client.get_check_intervals(table_id=table_id, start=start_date, end=end_date)[0]["latest_run_checks_job_id"]
        results = api_client.get_run_result(job_id=latest_job_id)
        tbl_homepage_url = anomalo_url + str(table_id)
        link_to_add = tbl_homepage_url
        link_description = "Anomalo Table Homepage"
        now = int(time.time() * 1000)  # milliseconds since epoch
        current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")

        institutional_memory_element = InstitutionalMemoryMetadataClass(
            url=link_to_add,
            description=link_description,
            createStamp=current_timestamp,
        )

        # Set platform and assertion_dataPlatformInstance values based on the platform mappings below
        # TODO: See if mapping below can be grabbed from https://app.anomalo.com/api/public/v1/list_warehouses
        platform_mapping = {'Snowflake': 'snowflake', 'qbiz-snowflake-sandbox-pipeline': 'snowflake', 'qbiz-bigquery-sbx-pipeline': 'bigquery', 'qbiz-bigquery-sandbox-pipeline': 'bigquery'}
        platform = platform_mapping[anomalo_warehouse_name]
        assertion_dataPlatformInstance = assertion_dataPlatformInstances[platform]

        # Below adds link back to Anomalo's table homepage in Datahub's Documentation section
        add_link(institutional_memory_element, link_to_add, dhtbl, platform, graph)

        # Below pushes each individual check to Datahub's Validation section
        for i in results['check_runs']:
            check_section_mcp = EmitMCP(i['run_config']['_metadata']['check_message'], dhtbl, emitter, assertion_dataPlatformInstance, platform)
            check_section_mcp.run()
            assertion_pass_fail(i['run_config']['_metadata']['check_message'], dhtbl,"pass" if (i['results']['success'] == True) else "fail", i['check_run_url'], i['results']['evaluated_message'], platform, emitter)