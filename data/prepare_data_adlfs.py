import adlfs
import ray.data
import pyarrow as pa

STORAGE_ACCOUNT_NAME = "kyleknappraystorage"
DATASET_NAME = "openwebtext"
CONTAINER_NAME = "datasets"


def get_blob_filesystem():
    fsspec_fs = adlfs.AzureBlobFileSystem(
        account_name=STORAGE_ACCOUNT_NAME,
        account_key="<account_key_here>",
    )
    return fsspec_fs


def convert_arrow_binary_to_table(batch):
    with pa.ipc.open_stream(batch["bytes"][0]) as reader:
        return reader.read_all()


def main():
    ray.init(address="auto")
    # ray.init() # To do local testing uncomment
    filesystem = get_blob_filesystem()

    raw_path = f"{CONTAINER_NAME}/{DATASET_NAME}/raw/"
    output_path = f"{CONTAINER_NAME}/{DATASET_NAME}/raydata_parquet/"

    print(f"Processing {DATASET_NAME} dataset with Ray Data...")
    print(f"Available Ray resources: {ray.cluster_resources()}")
    print(f"Input path: {raw_path}")
    print(f"Output path: {output_path}")

    try:
        print("Loading dataset from Azure Blob Storage...")
        ds = ray.data.read_binary_files(
            raw_path,
            filesystem=filesystem,
        )

        print("Converting binary Arrow data to PyArrow table...")
        ds_table = ds.map_batches(convert_arrow_binary_to_table, batch_size=1)

        print("Writing to Parquet format in Azure Blob Storage...")
        ds_table.write_parquet(output_path, filesystem=filesystem)

        print(f"✅ Dataset successfully converted and saved to: {output_path}")

    except Exception as e:
        print(f"❌ Error in Ray Data processing: {e}")
        raise
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
