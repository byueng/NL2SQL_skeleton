# -*- coding: utf-8 -*-
# @Time    : 2025-07-17 15:30
# @Author  : jwm
# @File    : main.py
# @description: For the High King

import argparse
import json
from typing import List, Dict, Any
from runner.run_manager import RunManager

def parse_augements() -> argparse.Namespace:
    args = argparse.ArgumentParser(description="")
    args.add_argument("--data_mode", type=str, required=True)
    args.add_argument("--data_path", type=str, required=True)
    args.add_argument("--model_path", type=str, required=True)
    args.add_argument("--schema_generator", type=str, required=True)
    args = args.parse_args()
    return args

def parse_augements_debug() -> argparse.Namespace:
    """
        Parameter parsing function for debugging, directly returns the preset parameter values.
        These default values ​​can be modified according to debugging needs.
    """

    debug_args = argparse.Namespace()
    debug_args.data_mode = "dev" 
    debug_args.data_path = "../dev/"  
    debug_args.model_path = "./src/llm/models.json"
    debug_args.schema_generator = "DDL"
    
    print(f"[DEBUG] Using Debug")
    print(f"[DEBUG] data_mode: {debug_args.data_mode}")
    print(f"[DEBUG] data_path: {debug_args.data_path}")
    
    return debug_args

def load_dataset(data_path: str) -> List[Dict[str, Any]]:
    """
    Loads the dataset from the specified path.
    Args:
        data_path (str): Path to the data file.

    Returns:
        List[Dict[str, Any]]: The loaded dataset.
    """
    with open(data_path, 'r') as file:
        dataset = json.load(file)
    return dataset

def main():
    # Debug model, if True using debug, or use cli model.
    DEBUG_MODE = True
    if DEBUG_MODE:
        args = parse_augements_debug()
    else:
        args = parse_augements()
        
    dataset = load_dataset(args.data_path + f"{args.data_mode}.json")
    runner = RunManager(args)
    runner.initialize_tasks(dataset)
    runner.run_task()

if __name__ == "__main__":
    main()