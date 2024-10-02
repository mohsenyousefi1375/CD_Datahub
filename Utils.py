from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn, make_dataset_urn_with_platform_instance, make_user_urn, make_group_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph, DataHubGraphConfig
from datahub.api.entities.corpuser.corpuser import CorpUser, CorpUserGenerationConfig
import logging
import time
import subprocess
from datahub.metadata.urns import CorpUserUrn
from typing import Optional

from datahub.api.entities.corpgroup.corpgroup import (
    CorpGroup,
    CorpGroupGenerationConfig,
)


# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DateTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    BooleanTypeClass,
    FixedTypeClass,
    BytesTypeClass,
    NumberTypeClass,
    DateTypeClass,
    TimeTypeClass, 
    EnumTypeClass, 
    NullTypeClass, 
    MapTypeClass, 
    ArrayTypeClass, 
    UnionTypeClass, 
    RecordTypeClass,
    EditableDatasetPropertiesClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,

)






def check_dataset_exists (dataset_urn): 
    text =f'datahub exists --urn "{dataset_urn}"'

    p = subprocess.Popen(text, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        if (line):
            return (line)
        else:
            return "dataset does not exits"
    retval = p.wait()


def find_datahub_data_type(physical_data_type:str):

    physical_data_type = str.lower(physical_data_type)

    if any(x in physical_data_type for x in ["varchar", "nvarchar", "text", "ntext", "string", "json", "xml"]):
        return StringTypeClass()
    
    elif any(x in physical_data_type for x in ["bool", "bit", "boolean"]):
        return BooleanTypeClass()
    
    elif any(x in physical_data_type for x in ["binary", "varbinary", "image"]):
        return BytesTypeClass()
    
    elif any(x in physical_data_type for x in ["int", "decimal", "number", "numeric"]):
        return NumberTypeClass()
    
    elif any(x in physical_data_type for x in ["date", "timestamp"]):
        return DateTypeClass()

    elif any(x in physical_data_type for x in ["time"]):
        return TimeTypeClass()
    
    else: NullTypeClass()

def extract_fileds_from_yaml(data):
    fields = []
    for column_name, column_information in data["dataset"]["columns"].items():
        
        description = f'{column_information["description"]}nAdditional informatin\nisEncrypted: {column_information["isEncrypted"]}\ndefinitions: {column_information["definitions"]}'
        
        fields.append(
            SchemaFieldClass(
                fieldPath = column_name,
                type = SchemaFieldDataTypeClass(type=find_datahub_data_type(column_information["physicalDataType"])),
                nativeDataType = column_information["physicalDataType"], 
                nullable = column_information["isNullable"],
                isPartOfKey = column_information["isPrimaryKey"],
                isPartitioningKey = column_information["isPartitionKey"],
                description = description,
                
                lastModified = AuditStampClass(
                    time = 1640692800000, actor = "urn:li:corpuser:ingestion"
                ),
            )
        )
    return fields


def add_dataset(data):
    gms_server = "http://localhost:8080"
    fields = extract_fileds_from_yaml (data)
    schema_name = data["dataset"]["schemaName"]
    platform = str.lower(data["dataset"]["platform"])
    dataset_name = data["dataset"]["schemaName"] + '.' + data["dataset"]["tableName"]
    description = data["dataset"]["description"]
    links = data["dataset"]["links"]
    environment = str.lower( data["dataset"]["environment"] )
    platform_instance = str.replace(str.replace(str.replace(str.replace(str.replace( data["dataset"]["location"], ',', '-'), ':', '-'), '(', ''), ')', ''), '.', '_')
    dataset_urn = make_dataset_urn_with_platform_instance(platform=platform, name=str.lower(dataset_name), env=environment, platform_instance= platform_instance)
    datahub_group_name = data["dataset"]["technicalOwner"]["datahub_group_name"]
    datahub_users = []
    datahub_users.append(data["dataset"]["technicalOwner"]["productManagerEmail"])


    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=SchemaMetadataClass(
            schemaName = schema_name,  # not used
            platform = make_data_platform_urn(platform),  # important <- platform must be an urn
            version=0,  # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
            hash = "",  # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
            platformSchema=OtherSchemaClass(rawSchema="__insert raw schema here__"),
            lastModified=AuditStampClass(
                time=1640692800000, actor="urn:li:corpuser:ingestion"
            ),
            fields = fields,
        ),
    )

    # Create rest emitter
    rest_emitter = DatahubRestEmitter(gms_server = gms_server)
    result = rest_emitter.emit(event)
    print (result)

    

    # add referenced_links
    for key, value in links.items():
        add_referenced_links_to_dataset(link_to_add= value["link"], link_description= value["description"],dataset_urn= dataset_urn, gms_endpoint= gms_server )

    # add documentation
    add_documentation_to_dataset(description, dataset_urn= dataset_urn, gms_endpoint=gms_server)

    # check datahub group is exists or new to add
    #create_datahub_group(display_name = datahub_group_name, gms_server=gms_server, list_of_members = datahub_users)


    #add technical owner
    add_dataset_owner( None,datahub_group_name, dataset_urn, gms_server, OwnershipTypeClass.TECHNICAL_OWNER)


def add_documentation_to_dataset(dataset_documentation, dataset_urn, gms_endpoint):
    # validate description
    if (len(dataset_documentation)<8):
        return
    
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    documentation_to_add = dataset_documentation


    # Some helpful variables to fill out objects later
    now = int(time.time() * 1000)  # milliseconds since epoch
    current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")

    # First we get the current owners
    graph = DataHubGraph(config=DatahubClientConfig(server=gms_endpoint))

    current_editable_properties = graph.get_aspect(
        entity_urn=dataset_urn, aspect_type=EditableDatasetPropertiesClass
    )

    need_write = False
    if current_editable_properties:
        if documentation_to_add != current_editable_properties.description:
            current_editable_properties.description = documentation_to_add
            need_write = True
    else:
        # create a brand new editable dataset properties aspect
        current_editable_properties = EditableDatasetPropertiesClass(
            created=current_timestamp, description=documentation_to_add
        )
        need_write = True

    if need_write:
        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=current_editable_properties,
        )
        graph.emit(event)
        log.info(f"Documentation added to dataset {dataset_urn}")
    else:
        log.info("Documentation already exists and is identical, omitting write")

def add_referenced_links_to_dataset(link_to_add:str, link_description:str, dataset_urn, gms_endpoint):

    # validate link
    if (len(link_to_add)<8):
        return
    
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)


    # Some helpful variables to fill out objects later
    now = int(time.time() * 1000)  # milliseconds since epoch
    current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")
    institutional_memory_element = InstitutionalMemoryMetadataClass(
        url=link_to_add,
        description=link_description,
        createStamp=current_timestamp,
    )

    graph = DataHubGraph(config=DatahubClientConfig(server=gms_endpoint))

    current_institutional_memory = graph.get_aspect(
        entity_urn=dataset_urn, aspect_type=InstitutionalMemoryClass
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
            entityUrn=dataset_urn,
            aspect=current_institutional_memory,
        )
        graph.emit(event)
        log.info(f"Link {link_to_add} added to dataset {dataset_urn}")

    else:
        log.info(f"Link {link_to_add} already exists and is identical, omitting write")


def create_datahub_user(display_name:str, user_email:str, title:str, first_name:str, last_name:str, full_name:str, gms_server ):

    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    user: CorpUser = CorpUser(
        id=user_email,
        display_name=display_name,
        email=user_email,
        title=title,
        first_name=first_name,
        last_name=last_name,
        full_name=full_name,
    )

    # Create graph client
    datahub_graph = DataHubGraph(DataHubGraphConfig(server=gms_server))
    for event in user.generate_mcp(
        generation_config=CorpUserGenerationConfig(override_editable=False)
    ):
        datahub_graph.emit(event)
    log.info(f"Upserted user {user.urn}")


def create_datahub_group( display_name:str , gms_server,list_of_members:list = [],group_email:str = None, description:str = None, slack = None ):
   
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    members=[]

    for i in list_of_members:
        members.append(CorpUserUrn(i))

    group = CorpGroup(
        id = group_email,
        owners = [str(CorpUserUrn("datahub"))],
        members = members,
        display_name=display_name,
        email=group_email,
        description = description,
        slack=slack,
    )

    # Create graph client
    datahub_graph = DataHubGraph(DataHubGraphConfig(server=gms_server))

    for event in group.generate_mcp(
        generation_config=CorpGroupGenerationConfig(
            override_editable=False, datahub_graph=datahub_graph
        )
    ):
        datahub_graph.emit(event)
    log.info(f"Upserted group {group.urn}")


 
def find_dataset_owner(gms_server, dataset_urn):

    graph = DataHubGraph(DatahubClientConfig(server=gms_server))

    # Query multiple aspects from entity
    result = graph.get_aspects_for_entity(
        entity_urn=dataset_urn,
        aspects=["ownership"],
        aspect_types=[OwnershipClass],
    )

    return result




def add_dataset_owner(datahub_user_name, datahub_group_name, dataset_urn, gms_server, ownership_type):
    
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)


    # Inputs -> owner, ownership_type, dataset
    if datahub_user_name:
        owner_to_add = make_user_urn(datahub_user_name)
    
    elif datahub_group_name:
        owner_to_add = make_user_urn(datahub_group_name)

    
    ownership_type =ownership_type #OwnershipTypeClass.TECHNICAL_OWNER


    # Some objects to help with conditional pathways later
    owner_class_to_add = OwnerClass(owner=owner_to_add, type=ownership_type)
    ownership_to_add = OwnershipClass(owners=[owner_class_to_add])


    graph = DataHubGraph(DatahubClientConfig(server=gms_server))


    current_owners: Optional[OwnershipClass] = graph.get_aspect(
        entity_urn=dataset_urn, aspect_type=OwnershipClass
    )


    need_write = False
    if current_owners:
        if (owner_to_add, ownership_type) not in [
            (x.owner, x.type) for x in current_owners.owners
        ]:
            # owners exist, but this owner is not present in the current owners
            current_owners.owners.append(owner_class_to_add)
            need_write = True
    else:
        # create a brand new ownership aspect
        current_owners = ownership_to_add
        need_write = True

    if need_write:
        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=current_owners,
        )
        graph.emit(event)
        log.info(
            f"Owner {owner_to_add}, type {ownership_type} added to dataset {dataset_urn}"
        )

    else:
        log.info(f"Owner {owner_to_add} already exists, omitting write")


def add_dataset_properties():
    pass