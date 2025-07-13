from pydantic import BaseModel, Field


class ModelFile(BaseModel):
    name: str
    description: str
    pipeline: list[FactoryType] = Field(
        description="A list of steps to be performed by the asset."
    )
    kinds: list[str] | None = None
    group_name: str | None = None
    key_prefix: dg.CoercibleToAssetKeyPrefix | None = None
    owners: list[str] | None = None
    tags: dict[str, str] | None = None


class AssetArgs(BaseModel):
    name: str
    compute_fn: str
    key_prefix: dg.CoercibleToAssetKeyPrefix | None = None
    owners: list[str] | None = None
    kinds: list[str] | None = None
    description: str | None = None
    tags: dict[str, str] | None = None
    group_name: str | None = None
    # ins: dict[str, AssetIn] | None = None
    # deps: list[CoercibleToAssetDep] | None = None
    # metadata: ArbitraryMetadataMapping | None = None
    # config_schema: UserConfigSchema | None = None
    # required_resource_keys: AbstractSet[str] | None = None
    # resource_defs: dict[str, object] | None = None
    # io_manager_def: object | None = None
    # io_manager_key: str | None = None
    # dagster_type: DagsterType | None = None
    # partitions_def: PartitionsDefinition | None = None
    # op_tags: Mapping[str, Any] | None = None
    # output_required: bool = True
    # automation_condition: AutomationCondition | None = None
    # backfill_policy: BackfillPolicy | None = None
    # retry_policy: RetryPolicy | None = None
    # code_version: str | None = None
    # key: CoercibleToAssetKey | None = None
    # check_specs: Sequence[AssetCheckSpec] | None = None

    # model_config = ConfigDict(
    #     extra='allow',
    # )


class Project(BaseModel):
    name: str
    version: str
    profile: str = Field(
        description="This setting configures which 'profile' dbt uses for this project."
    )
    model_paths: list[str] = Field(
        description="The paths to the model files."
    )
    macro_paths: list[str] = Field(
        description="The paths to the macro files."
    )
    # models: ModelConfig


def parse_model():
    pass
