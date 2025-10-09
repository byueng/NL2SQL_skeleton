source .env

db_mode=$DB_MODE
db_path=$DB_ROOT
model_path=$MODEL_PATH
schema_generator=$SCHEMA_GENERATOR

# set tqdm progress 
export TRANSFORMERS_NO_TQDM=1
export HF_HUB_DISABLE_PROGRESS_BARS=1

python ./src/main.py --data_mode "$db_mode" \
                     --data_path "$db_path" \
                     --model_path "$model_path" \
                     --schema_generator "$schema_generator" \
