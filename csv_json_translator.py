import asyncio
import json
import csv
import os
import argparse
import time
import logging
from typing import Dict, List, Any, Tuple, Optional, Set, Union
from dataclasses import dataclass, asdict, field
import aiohttp
import urllib.parse
import random

# Re-use the existing AsyncTranslator class and its components from translator.py
from translator import (
    AsyncTranslator, TranslationResult, setup_logging
)

@dataclass
class TranslationStats:
    """Statistics for translation tasks."""
    total_items_processed: int = 0
    total_fields_processed: int = 0
    total_texts_translated: int = 0
    successful_translations: int = 0
    failed_translations: int = 0
    cached_translations: int = 0
    start_time: float = field(default_factory=time.time)
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @property
    def success_rate(self) -> float:
        return self.successful_translations / self.total_texts_translated if self.total_texts_translated else 0
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["elapsed_time"] = self.elapsed_time
        result["success_rate"] = self.success_rate
        return result

class FileTranslator:
    """
    Handles translation of entire files (JSON, CSV) while preserving structure.
    Uses AsyncTranslator for the actual translation work.
    """
    
    def __init__(self, 
                 translator: Optional[AsyncTranslator] = None,
                 max_concurrent_requests: int = 10,
                 logger: Optional[logging.Logger] = None,
                 include_fields: Optional[List[str]] = None,
                 exclude_fields: Optional[List[str]] = None,
                 max_text_length: int = 5000):
        """
        Initialize the file translator.
        
        Args:
            translator: AsyncTranslator instance or None to create a new one
            max_concurrent_requests: Maximum number of concurrent requests
            logger: Logger instance or None to create a new one
            include_fields: List of fields to translate (if None, translate all)
            exclude_fields: List of fields to exclude from translation
            max_text_length: Maximum length of text to translate in one request
        """
        self.translator = translator or AsyncTranslator(max_concurrent_requests=max_concurrent_requests)
        self.logger = logger or setup_logging()
        self.include_fields = include_fields
        self.exclude_fields = exclude_fields
        self.max_text_length = max_text_length
        self.stats = TranslationStats()
        
    def should_translate_field(self, field_name: str) -> bool:
        """Determine if a field should be translated based on include/exclude lists."""
        # If include_fields is specified, only translate those fields
        if self.include_fields is not None:
            return field_name in self.include_fields
            
        # If exclude_fields is specified, don't translate those fields
        if self.exclude_fields is not None:
            return field_name not in self.exclude_fields
            
        # By default, translate all fields
        return True
    
    async def translate_csv(self, 
                           input_file: str,
                           output_file: str,
                           src_lang: str = 'auto',
                           dest_lang: str = 'en',
                           batch_size: int = 50,
                           has_header: bool = True,
                           translate_headers: bool = False,
                           delimiter: str = ',',
                           quotechar: str = '"') -> Dict[str, Any]:
        """
        Translates a CSV file while preserving its structure.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            src_lang: Source language code
            dest_lang: Destination language code
            batch_size: Batch size for translation
            has_header: Whether the CSV has a header row
            translate_headers: Whether to translate the header row
            delimiter: CSV delimiter
            quotechar: CSV quote character
            
        Returns:
            Dictionary with translation statistics
        """
        self.logger.info(f"Starting CSV translation: {input_file} -> {output_file}")
        self.stats = TranslationStats()  # Reset stats
        
        # Read the entire CSV file
        rows = []
        header = None
        field_indices_to_translate = []
        
        try:
            with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
                
                # Handle header if present
                if has_header:
                    header = next(reader)
                    
                    # Determine which columns to translate
                    field_indices_to_translate = [
                        i for i, field_name in enumerate(header)
                        if self.should_translate_field(field_name)
                    ]
                    
                    self.logger.info(f"Found {len(header)} columns, {len(field_indices_to_translate)} will be translated")
                else:
                    # If no header, assume all columns should be translated
                    # We'll determine the number of columns from the first row
                    first_row = next(reader)
                    field_indices_to_translate = list(range(len(first_row)))
                    rows.append(first_row)
                    self.logger.info(f"No header, found {len(field_indices_to_translate)} columns")
                    
                # Read all rows
                for row in reader:
                    rows.append(row)
        
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {str(e)}")
            raise
            
        self.logger.info(f"Read {len(rows)} rows from CSV file")
        self.stats.total_items_processed = len(rows)
        
        # Prepare texts for translation
        texts_to_translate = []
        text_mapping = {}  # Maps (row_idx, col_idx) to text_idx
        
        # Handle header translation if needed
        header_translations = {}
        if has_header and translate_headers and header:
            header_texts = [header[i] for i in field_indices_to_translate]
            self.logger.info(f"Translating {len(header_texts)} header fields")
            
            header_results = await self.translator.batch_translate_large(
                header_texts, src_lang, dest_lang, batch_size
            )
            
            for i, result in enumerate(header_results):
                original_idx = field_indices_to_translate[i]
                header_translations[original_idx] = result.translated_text if result.success else header[original_idx]
                
                if result.success:
                    self.stats.successful_translations += 1
                else:
                    self.stats.failed_translations += 1
                    
            self.stats.total_texts_translated += len(header_texts)
            
        # Collect all texts to translate
        for row_idx, row in enumerate(rows):
            for col_idx in field_indices_to_translate:
                if col_idx < len(row) and row[col_idx]:
                    # Only add non-empty cells
                    text = row[col_idx]
                    if isinstance(text, str) and text.strip():
                        text_idx = len(texts_to_translate)
                        texts_to_translate.append(text)
                        text_mapping[(row_idx, col_idx)] = text_idx
        
        self.logger.info(f"Collected {len(texts_to_translate)} text cells to translate")
        self.stats.total_fields_processed = len(texts_to_translate)
        
        # Translate in batches
        results = []
        if texts_to_translate:
            results = await self.translator.batch_translate_large(
                texts_to_translate, src_lang, dest_lang, batch_size
            )
            self.stats.total_texts_translated += len(texts_to_translate)
            
            # Count successes and failures
            for result in results:
                if result.success:
                    self.stats.successful_translations += 1
                else: 
                    self.stats.failed_translations += 1
        
        # Write the translated CSV
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter=delimiter, quotechar=quotechar)
                
                # Write header if present
                if has_header and header:
                    if translate_headers:
                        translated_header = [
                            header_translations.get(i, header[i]) 
                            for i in range(len(header))
                        ]
                        writer.writerow(translated_header)
                    else:
                        writer.writerow(header)
                
                # Write translated rows
                for row_idx, row in enumerate(rows):
                    translated_row = list(row)  # Make a copy to modify
                    
                    for col_idx in field_indices_to_translate:
                        if (row_idx, col_idx) in text_mapping:
                            text_idx = text_mapping[(row_idx, col_idx)]
                            if text_idx < len(results) and results[text_idx].success:
                                translated_row[col_idx] = results[text_idx].translated_text
                    
                    writer.writerow(translated_row)
        
        except Exception as e:
            self.logger.error(f"Error writing translated CSV: {str(e)}")
            raise
            
        elapsed = self.stats.elapsed_time
        self.logger.info(
            f"CSV translation completed in {elapsed:.2f}s. "
            f"Translated {self.stats.total_texts_translated} texts with "
            f"{self.stats.success_rate * 100:.1f}% success rate."
        )
        
        return self.stats.to_dict()
                
    async def translate_json(self,
                            input_file: str,
                            output_file: str,
                            src_lang: str = 'auto',
                            dest_lang: str = 'en',
                            batch_size: int = 50) -> Dict[str, Any]:
        """
        Translates a JSON file while preserving its structure.
        
        Args:
            input_file: Path to input JSON file
            output_file: Path to output JSON file
            src_lang: Source language code
            dest_lang: Destination language code
            batch_size: Batch size for translation
            
        Returns:
            Dictionary with translation statistics
        """
        self.logger.info(f"Starting JSON translation: {input_file} -> {output_file}")
        self.stats = TranslationStats()  # Reset stats
        
        # Read JSON file
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            self.logger.error(f"Error reading JSON file: {str(e)}")
            raise
            
        # Extract all translatable text fields
        texts_to_translate = []
        text_paths = []
        
        def extract_text(obj: Any, path: List[Union[str, int]] = None) -> None:
            """Recursively extract all text values from a JSON object."""
            if path is None:
                path = []
                
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = path + [key]
                    # Check if this field should be translated
                    field_name = key
                    if self.should_translate_field(field_name):
                        if isinstance(value, str) and value.strip():
                            if len(value) <= self.max_text_length:
                                texts_to_translate.append(value)
                                text_paths.append(new_path)
                            else:
                                self.logger.warning(f"Text at {new_path} is too long ({len(value)} chars), skipping")
                        elif isinstance(value, (dict, list)):
                            extract_text(value, new_path)
                    else:
                        # Even if we don't translate this field, we need to recurse if it's a container
                        if isinstance(value, (dict, list)):
                            extract_text(value, new_path)
            
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    new_path = path + [i]
                    if isinstance(item, str) and item.strip():
                        if len(item) <= self.max_text_length:
                            texts_to_translate.append(item)
                            text_paths.append(new_path)
                        else:
                            self.logger.warning(f"Text at {new_path} is too long ({len(item)} chars), skipping")
                    elif isinstance(item, (dict, list)):
                        extract_text(item, new_path)
        
        # Extract all translatable texts
        extract_text(data)
        
        self.logger.info(f"Found {len(texts_to_translate)} text fields to translate")
        self.stats.total_items_processed = 1  # One JSON file
        self.stats.total_fields_processed = len(texts_to_translate)
        
        # Translate all texts
        results = []
        if texts_to_translate:
            results = await self.translator.batch_translate_large(
                texts_to_translate, src_lang, dest_lang, batch_size
            )
            self.stats.total_texts_translated = len(texts_to_translate)
            
            # Count successes and failures
            for result in results:
                if result.success:
                    self.stats.successful_translations += 1
                else:
                    self.stats.failed_translations += 1
        
        # Apply translations back to JSON structure
        def update_json(obj: Any, path: List[Union[str, int]], depth: int, value: str) -> None:
            """Update a value at a specific path in a JSON object."""
            if depth == len(path) - 1:
                key = path[depth]
                if isinstance(obj, dict) and key in obj:
                    obj[key] = value
                elif isinstance(obj, list) and isinstance(key, int) and 0 <= key < len(obj):
                    obj[key] = value
                return
                
            key = path[depth]
            if isinstance(obj, dict) and key in obj:
                update_json(obj[key], path, depth + 1, value)
            elif isinstance(obj, list) and isinstance(key, int) and 0 <= key < len(obj):
                update_json(obj[key], path, depth + 1, value)
        
        # Apply translations
        for i, result in enumerate(results):
            if result.success:
                path = text_paths[i]
                update_json(data, path, 0, result.translated_text)
        
        # Write translated JSON
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Error writing translated JSON: {str(e)}")
            raise
            
        elapsed = self.stats.elapsed_time
        self.logger.info(
            f"JSON translation completed in {elapsed:.2f}s. "
            f"Translated {self.stats.total_texts_translated} texts with "
            f"{self.stats.success_rate * 100:.1f}% success rate."
        )
        
        return self.stats.to_dict()
    
    async def translate_jsonl(self,
                             input_file: str,
                             output_file: str,
                             src_lang: str = 'auto',
                             dest_lang: str = 'en',
                             batch_size: int = 50) -> Dict[str, Any]:
        """
        Translates a JSON Lines file (one JSON object per line).
        
        Args:
            input_file: Path to input JSONL file
            output_file: Path to output JSONL file
            src_lang: Source language code
            dest_lang: Destination language code
            batch_size: Batch size for translation
            
        Returns:
            Dictionary with translation statistics
        """
        self.logger.info(f"Starting JSONL translation: {input_file} -> {output_file}")
        self.stats = TranslationStats()  # Reset stats
        
        # Read all lines
        lines = []
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        lines.append(line.strip())
        except Exception as e:
            self.logger.error(f"Error reading JSONL file: {str(e)}")
            raise
            
        self.logger.info(f"Read {len(lines)} JSON objects from file")
        self.stats.total_items_processed = len(lines)
        
        # Process each JSON object separately
        output_lines = []
        
        for line_idx, line in enumerate(lines):
            try:
                # Parse JSON object
                obj = json.loads(line)
                
                # Extract texts
                texts_to_translate = []
                text_paths = []
                
                def extract_text(obj: Any, path: List[Union[str, int]] = None) -> None:
                    """Extract all translatable text from a JSON object."""
                    if path is None:
                        path = []
                        
                    if isinstance(obj, dict):
                        for key, value in obj.items():
                            new_path = path + [key]
                            field_name = key
                            if self.should_translate_field(field_name):
                                if isinstance(value, str) and value.strip():
                                    if len(value) <= self.max_text_length:
                                        texts_to_translate.append(value)
                                        text_paths.append(new_path)
                                    else:
                                        self.logger.warning(f"Text at {new_path} is too long ({len(value)} chars), skipping")
                                elif isinstance(value, (dict, list)):
                                    extract_text(value, new_path)
                            else:
                                if isinstance(value, (dict, list)):
                                    extract_text(value, new_path)
                    
                    elif isinstance(obj, list):
                        for i, item in enumerate(obj):
                            new_path = path + [i]
                            if isinstance(item, str) and item.strip():
                                if len(item) <= self.max_text_length:
                                    texts_to_translate.append(item)
                                    text_paths.append(new_path)
                                else:
                                    self.logger.warning(f"Text at {new_path} is too long ({len(item)} chars), skipping")
                            elif isinstance(item, (dict, list)):
                                extract_text(item, new_path)
                
                # Extract texts from this JSON object
                extract_text(obj)
                
                if texts_to_translate:
                    self.logger.info(f"Object {line_idx+1}: Found {len(texts_to_translate)} text fields to translate")
                    self.stats.total_fields_processed += len(texts_to_translate)
                    
                    # Translate texts
                    results = await self.translator.batch_translate_large(
                        texts_to_translate, src_lang, dest_lang, batch_size
                    )
                    self.stats.total_texts_translated += len(texts_to_translate)
                    
                    # Count successes and failures
                    for result in results:
                        if result.success:
                            self.stats.successful_translations += 1
                        else:
                            self.stats.failed_translations += 1
                    
                    # Apply translations
                    def update_json(obj: Any, path: List[Union[str, int]], depth: int, value: str) -> None:
                        """Update a value at a specific path in a JSON object."""
                        if depth == len(path) - 1:
                            key = path[depth]
                            if isinstance(obj, dict) and key in obj:
                                obj[key] = value
                            elif isinstance(obj, list) and isinstance(key, int) and 0 <= key < len(obj):
                                obj[key] = value
                            return
                            
                        key = path[depth]
                        if isinstance(obj, dict) and key in obj:
                            update_json(obj[key], path, depth + 1, value)
                        elif isinstance(obj, list) and isinstance(key, int) and 0 <= key < len(obj):
                            update_json(obj[key], path, depth + 1, value)
                    
                    # Apply translations
                    for i, result in enumerate(results):
                        if result.success:
                            path = text_paths[i]
                            update_json(obj, path, 0, result.translated_text)
                
                # Serialize JSON object back to string
                output_lines.append(json.dumps(obj, ensure_ascii=False))
                
            except json.JSONDecodeError as e:
                self.logger.error(f"Error parsing JSON object at line {line_idx+1}: {str(e)}")
                # Preserve the original line if we can't parse it
                output_lines.append(line)
            except Exception as e:
                self.logger.error(f"Error processing JSON object at line {line_idx+1}: {str(e)}")
                output_lines.append(line)
        
        # Write output file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for line in output_lines:
                    f.write(line + '\n')
        except Exception as e:
            self.logger.error(f"Error writing translated JSONL: {str(e)}")
            raise
            
        elapsed = self.stats.elapsed_time
        self.logger.info(
            f"JSONL translation completed in {elapsed:.2f}s. "
            f"Translated {self.stats.total_texts_translated} texts with "
            f"{self.stats.success_rate * 100:.1f}% success rate."
        )
        
        return self.stats.to_dict()

    async def translate_file(self,
                           input_file: str,
                           output_file: str, 
                           src_lang: str = 'auto',
                           dest_lang: str = 'en',
                           file_type: Optional[str] = None,
                           batch_size: int = 50,
                           **kwargs) -> Dict[str, Any]:
        """
        Unified method to translate any supported file type.
        Will detect file type from extension if not specified.
        
        Args:
            input_file: Path to input file
            output_file: Path to output file
            src_lang: Source language code
            dest_lang: Destination language code
            file_type: Force specific file type handling ('json', 'csv', 'jsonl')
            batch_size: Batch size for translations
            **kwargs: Additional arguments for specific file types
            
        Returns:
            Dictionary with translation statistics
        """
        # Determine file type if not specified
        if file_type is None:
            ext = os.path.splitext(input_file)[1].lower()
            if ext == '.json':
                file_type = 'json'
            elif ext == '.csv':
                file_type = 'csv'
            elif ext == '.jsonl' or ext == '.ndjson':
                file_type = 'jsonl'
            else:
                raise ValueError(f"Unable to determine file type from extension: {ext}")
        
        self.logger.info(f"Translating {file_type} file: {input_file} -> {output_file}")
        
        # Call the appropriate translator method
        if file_type == 'json':
            return await self.translate_json(input_file, output_file, src_lang, dest_lang, batch_size)
        elif file_type == 'csv':
            return await self.translate_csv(input_file, output_file, src_lang, dest_lang, batch_size, **kwargs)
        elif file_type == 'jsonl':
            return await self.translate_jsonl(input_file, output_file, src_lang, dest_lang, batch_size)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")


async def main():
    """Main entry point with command line argument parsing."""
    parser = argparse.ArgumentParser(description='Translate JSON, JSONL, and CSV files while preserving structure.')
    
    # Required arguments
    parser.add_argument('input_file', help='Path to input file')
    parser.add_argument('output_file', help='Path to output file')
    
    # Optional arguments
    parser.add_argument('--src-lang', default='auto', help='Source language code (default: auto)')
    parser.add_argument('--dest-lang', default='en', help='Destination language code (default: en)')
    parser.add_argument('--file-type', choices=['json', 'jsonl', 'csv'], help='Force specific file type')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for translation (default: 50)')
    parser.add_argument('--concurrent', type=int, default=10, help='Maximum concurrent requests (default: 10)')
    parser.add_argument('--log-file', default='file_translation.log', help='Log file path (default: file_translation.log)')
    
    # CSV-specific arguments
    parser.add_argument('--has-header', action='store_true', help='CSV has header row (default: True)')
    parser.add_argument('--no-header', dest='has_header', action='store_false', help='CSV does not have header row')
    parser.add_argument('--translate-headers', action='store_true', help='Translate CSV headers (default: False)')
    parser.add_argument('--delimiter', default=',', help='CSV delimiter (default: ,)')
    parser.add_argument('--quotechar', default='"', help='CSV quote character (default: ")')
    
    # Field filtering arguments
    parser.add_argument('--include-fields', nargs='+', help='Only translate these fields (comma or space separated)')
    parser.add_argument('--exclude-fields', nargs='+', help='Do not translate these fields (comma or space separated)')
    
    # Set default for has_header
    parser.set_defaults(has_header=True)
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging(args.log_file)
    
    # Process include/exclude fields
    include_fields = None
    exclude_fields = None
    
    if args.include_fields:
        include_fields = []
        for field in args.include_fields:
            if ',' in field:
                include_fields.extend([f.strip() for f in field.split(',')])
            else:
                include_fields.append(field.strip())
                
    if args.exclude_fields:
        exclude_fields = []
        for field in args.exclude_fields:
            if ',' in field:
                exclude_fields.extend([f.strip() for f in field.split(',')])
            else:
                exclude_fields.append(field.strip())
    
    # Create translator
    translator = AsyncTranslator(max_concurrent_requests=args.concurrent, logger=logger)
    file_translator = FileTranslator(
        translator=translator,
        logger=logger,
        include_fields=include_fields,
        exclude_fields=exclude_fields
    )
    
    # Run translation
    try:
        stats = await file_translator.translate_file(
            input_file=args.input_file,
            output_file=args.output_file,
            src_lang=args.src_lang,
            dest_lang=args.dest_lang,
            file_type=args.file_type,
            batch_size=args.batch_size,
            has_header=args.has_header,
            translate_headers=args.translate_headers,
            delimiter=args.delimiter,
            quotechar=args.quotechar
        )
        
        logger.info("Translation statistics:")
        for key, value in stats.items():
            if isinstance(value, float):
                logger.info(f"  {key}: {value:.2f}")
            else:
                logger.info(f"  {key}: {value}")
                
        logger.info(f"✓ Translation completed successfully: {args.input_file} -> {args.output_file}")
        
    except Exception as e:
        logger.error(f"Translation failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
        
    return 0

if __name__ == "__main__":
    # Set up asyncio policies for Windows if needed
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy') and hasattr(asyncio, 'set_event_loop_policy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    exit_code = asyncio.run(main())
    exit(exit_code)
