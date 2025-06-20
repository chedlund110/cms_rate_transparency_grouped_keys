import os
import shutil
from datetime import datetime
from io import BytesIO, BufferedWriter
from shared_config import SharedConfig
from constants import MAX_FILE_SIZE
from utilities import load_config

def merge_all_outputs(shared_config: SharedConfig) -> None:
    print("\n🧩 Starting merge of all output files...")

    merge_provider_identifiers(shared_config)
    merge_contract_xrefs(shared_config)
    merge_negotiated_rate_files(shared_config)

    print("✅ All output files merged successfully.")

def merge_provider_identifiers(shared_config: SharedConfig) -> None:
    print("🔀 Merging provider identifiers...")

    prefix = shared_config.mrf_file_prefixes["provider_identifier"]
    ext = shared_config.mrf_file_prefixes["provider_identifier_ext"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_path = os.path.join(shared_config.mrf_target_directory, f"{prefix}{timestamp}.{ext}")

    temp_output_dir = shared_config.directory_structure["temp_output_dir"]

    with open(output_file_path, "w", encoding="utf-8") as outfile:
        for root, _, files in os.walk(temp_output_dir):
            for file in files:
                if file == "identifiers.txt":
                    full_path = os.path.join(root, file)
                    try:
                        with open(full_path, "r", encoding="utf-8") as infile:
                            shutil.copyfileobj(infile, outfile)
                    except Exception as e:
                        print(f"⚠️ Skipping unreadable file {full_path}: {e}")

    create_mms_file(output_file_path, count_file_lines(output_file_path))

def merge_contract_xrefs(shared_config: SharedConfig) -> None:
    print("🔀 Merging provider group contract xrefs...")

    prefix = shared_config.mrf_file_prefixes["prov_grp_contract"]
    ext = shared_config.mrf_file_prefixes["prov_grp_contract_ext"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_path = os.path.join(shared_config.mrf_target_directory, f"{prefix}{timestamp}.{ext}")

    temp_output_dir = shared_config.directory_structure["temp_output_dir"]

    with open(output_file_path, "w", encoding="utf-8") as outfile:
        for root, _, files in os.walk(temp_output_dir):
            for file in files:
                if file == "contractxref.txt":
                    full_path = os.path.join(root, file)
                    try:
                        with open(full_path, "r", encoding="utf-8") as infile:
                            shutil.copyfileobj(infile, outfile)
                    except Exception as e:
                        print(f"⚠️ Skipping unreadable file {full_path}: {e}")

    create_mms_file(output_file_path, count_file_lines(output_file_path))

def merge_negotiated_rate_files(shared_config: SharedConfig) -> None:

    MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5 GB
    MAX_BUFFER_SIZE = 100 * 1024 * 1024     # 100 MB buffer flush limit
    
    print("🔀 Merging negotiated rate files with 5GB limit per file (buffered write)...")

    merged_file_index = 1
    current_file_size = 0
    current_record_count = 0
    buffer = BytesIO()

    prefix = shared_config.mrf_file_prefixes["negotiated_rate"]
    ext = shared_config.mrf_file_prefixes["negotiated_rate_ext"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    negotiated_dir = os.path.join(shared_config.directory_structure["temp_output_dir"], "negotiated")
    output_dir = shared_config.mrf_target_directory

    def create_new_output_file():
        nonlocal merged_file_index
        filename = f"{prefix}{timestamp}_{merged_file_index}.{ext}"
        merged_file_index += 1
        return os.path.join(output_dir, filename)

    def flush_buffer(file_path: str, buf: BytesIO):
        size = buf.tell()
        if size == 0:
            return  # nothing to flush
        try:
            with open(file_path, "ab") as f:
                with BufferedWriter(f, buffer_size=10_000_000) as out:
                    out.write(buf.getvalue())
                    out.flush()
            buf.seek(0)
            buf.truncate(0)
        except Exception as e:
            print(f"❌ Exception while flushing buffer to {file_path}: {e}")

    current_file_path = create_new_output_file()

    for file in sorted(os.listdir(negotiated_dir)):
        if file.endswith(".TXT") and file.startswith("NEGOTIATED"):
            full_path = os.path.join(negotiated_dir, file)
            try:
                with open(full_path, "r", encoding="utf-8") as infile:
                    for line in infile:
                        encoded_line = line.encode("utf-8")
                        line_size = len(encoded_line)

                        # If adding this line would exceed max file size, flush & rotate file first
                        if current_file_size + line_size > MAX_FILE_SIZE:
                            flush_buffer(current_file_path, buffer)
                            create_mms_file(current_file_path, current_record_count)
                            current_file_path = create_new_output_file()
                            current_file_size = 0
                            current_record_count = 0

                        buffer.write(encoded_line)
                        current_file_size += line_size
                        current_record_count += 1

                        # Flush early if buffer too big, to limit memory use
                        if buffer.tell() >= MAX_BUFFER_SIZE:
                            flush_buffer(current_file_path, buffer)

            except Exception as e:
                print(f"⚠️ Skipping unreadable or locked file {full_path}: {e}")

    # Final flush for remaining data
    flush_buffer(current_file_path, buffer)
    create_mms_file(current_file_path, current_record_count)

def create_mms_file(filename: str, rec_cnt: int) -> None:
    mms_filename = filename.rsplit(".", 1)[0] + ".MMS"
    with open(mms_filename, mode="w", encoding="utf-8") as f:
        f.write("Number of Records:" + str(rec_cnt))

def count_file_lines(file_path: str) -> int:
    with open(file_path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)

def get_shared_config():
    config = load_config("./config/config.json")
    if config is None:
        return None

    app_base_dir = config["app_base_directory"]

    directory_structure = {
        key: os.path.join(app_base_dir, path)
        for key, path in config["directory_structure"].items()
    }

    mrf_target_directory = os.path.join(app_base_dir, config["directory_structure"]["mrf_output_dir"])

    return SharedConfig(
        reporting_entity=config.get("reporting_entity", ""),
        reporting_entity_type=config.get("reporting_entity_type", ""),
        insurer_code=config.get("insurer_code", ""),
        program_list=config.get("programs", []),
        provider_code_range_types=config.get("provider_code_range_types", []),
        service_code_range_types=config.get("service_code_range_types", []),
        service_code_companion_range_types=config.get("service_companion_code_types", []),
        mrf_target_directory=mrf_target_directory,
        mrf_file_prefixes=config["mrf_file_prefixes"],
        provider_identifier_full_path="",
        prov_grp_contract_full_path="",
        networx_connection_string="",
        qnxt_connection_string="",
        directory_structure=directory_structure
    )

if __name__ == "__main__":
    shared_config = get_shared_config()
    if shared_config:
        merge_all_outputs(shared_config)