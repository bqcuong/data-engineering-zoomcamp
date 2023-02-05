from prefect.filesystems import GitHub

if __name__ == "__main__":
    block = GitHub(
        repository="https://github.com/bqcuong/etl-gcs",
    )
    block.get_directory(".")
    block.save("main")