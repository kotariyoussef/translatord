#!/usr/bin/env python3
"""
Django PO File Translator

This script automates the translation of Django PO (gettext) files using the AsyncTranslator.
It preserves the PO file format, including comments, headers, and msgctxt entries while
translating the msgid strings to the target language.

Usage:
    python po_translator.py -i input.po -o output.po -s en -t fr

Features:
- Preserves PO file format and metadata
- Handles plurals correctly
- Supports batch processing of multiple files
- Progress tracking and detailed reporting
- Skips already translated entries (optional)
- Handles fuzzy translations
"""

import os
import re
import sys
import argparse
import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional, Any, Union
from dataclasses import dataclass
import polib
from translator import AsyncTranslator  # Import the AsyncTranslator we created earlier

@dataclass
class TranslationStats:
    """Statistics about the translation process."""
    total_entries: int = 0
    translated_entries: int = 0
    skipped_entries: int = 0
    failed_entries: int = 0
    fuzzy_entries: int = 0
    time_taken: float = 0.0

class POTranslator:
    """
    A class to translate PO files while preserving their structure.
    Uses AsyncTranslator for the actual translations.
    """
    
    def __init__(self, 
                 source_lang: str = 'en', 
                 target_lang: str = 'es',
                 max_concurrent: int = 10,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize the PO translator.
        
        Args:
            source_lang: Source language code
            target_lang: Target language code
            max_concurrent: Maximum number of concurrent translation requests
            logger: Custom logger (if None, a default one will be created)
        """
        self.source_lang = source_lang
        self.target_lang = target_lang
        self.max_concurrent = max_concurrent
        
        # Set up logger
        if logger is None:
            self.logger = logging.getLogger('po_translator')
            self.logger.setLevel(logging.INFO)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
            self.logger.addHandler(console_handler)
        else:
            self.logger = logger
            
        # Initialize the async translator
        self.translator = AsyncTranslator(
            max_concurrent_requests=max_concurrent,
            logger=self.logger
        )
        
        # Cache for translations to avoid duplicates
        self.translation_cache = {}
        
    async def translate_po_file(self, 
                              input_file: str, 
                              output_file: str,
                              skip_translated: bool = True,
                              update_fuzzy: bool = True,
                              batch_size: int = 50) -> TranslationStats:
        """
        Translate a PO file while preserving its structure.
        
        Args:
            input_file: Path to input PO file
            output_file: Path to output PO file
            skip_translated: Skip entries that already have translations
            update_fuzzy: Update entries marked as fuzzy
            batch_size: Size of translation batches
            
        Returns:
            TranslationStats object with statistics about the translation process
        """
        start_time = time.time()
        stats = TranslationStats()
        
        try:
            # Load the PO file
            self.logger.info(f"Loading PO file: {input_file}")
            po = polib.pofile(input_file)
            
            # Update metadata
            self.update_po_metadata(po)
            
            # Extract entries that need translation
            entries_to_translate = []
            original_texts = []
            
            for entry in po:
                stats.total_entries += 1
                
                # Skip header
                if entry.msgid == '':
                    continue
                    
                # Check if we should translate this entry
                should_translate = False
                
                if not entry.translated():
                    should_translate = True
                elif entry.fuzzy and update_fuzzy:
                    should_translate = True
                    stats.fuzzy_entries += 1
                elif not skip_translated:
                    should_translate = True
                    
                if should_translate:
                    entries_to_translate.append(entry)
                    original_texts.append(entry.msgid)
                else:
                    stats.skipped_entries += 1
            
            # If there's nothing to translate, just save and exit
            if not entries_to_translate:
                self.logger.info("No entries to translate. Saving file as is.")
                po.save(output_file)
                stats.time_taken = time.time() - start_time
                return stats
                
            # Translate the texts
            self.logger.info(f"Translating {len(entries_to_translate)} entries from {self.source_lang} to {self.target_lang}")
            translation_results = await self.translator.batch_translate_large(
                texts=original_texts,
                src_lang=self.source_lang,
                dest_lang=self.target_lang,
                batch_size=batch_size
            )
            
            # Update the PO entries with translations
            for entry, result in zip(entries_to_translate, translation_results):
                if result.success and result.translated_text:
                    entry.msgstr = result.translated_text
                    
                    # If it was fuzzy and we updated it, we can remove the fuzzy flag
                    if entry.fuzzy and update_fuzzy:
                        entry.flags.remove('fuzzy')
                        
                    stats.translated_entries += 1
                else:
                    self.logger.warning(f"Failed to translate: {entry.msgid[:50]}...")
                    stats.failed_entries += 1
            
            # Handle plural forms
            await self.translate_plural_forms(po)
            
            # Save the translated file
            self.logger.info(f"Saving translated PO file to: {output_file}")
            po.save(output_file)
            
            stats.time_taken = time.time() - start_time
            
            # Log summary
            self.logger.info(
                f"Translation completed in {stats.time_taken:.2f}s: "
                f"{stats.translated_entries}/{stats.total_entries} entries translated, "
                f"{stats.skipped_entries} skipped, {stats.failed_entries} failed"
            )
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error translating PO file: {str(e)}")
            stats.time_taken = time.time() - start_time
            raise
    
    async def translate_plural_forms(self, po: polib.POFile) -> None:
        """
        Handle translation of plural forms in the PO file.
        
        Args:
            po: The polib.POFile object
        """
        plural_entries = [e for e in po if e.msgid_plural and not e.translated()]
        
        if not plural_entries:
            return
            
        self.logger.info(f"Translating {len(plural_entries)} plural forms")
        
        for entry in plural_entries:
            # Translate the singular form
            if not entry.msgstr_plural.get(0):
                results = await self.translator.batch_translate(
                    [entry.msgid],
                    self.source_lang,
                    self.target_lang
                )
                if results[0].success:
                    entry.msgstr_plural[0] = results[0].translated_text
            
            # Translate the plural form
            if not entry.msgstr_plural.get(1):
                results = await self.translator.batch_translate(
                    [entry.msgid_plural],
                    self.source_lang,
                    self.target_lang
                )
                if results[0].success:
                    entry.msgstr_plural[1] = results[0].translated_text
    
    def update_po_metadata(self, po: polib.POFile) -> None:
        """
        Update metadata in the PO file.
        
        Args:
            po: The polib.POFile object
        """
        # Update language
        if 'Language' in po.metadata:
            po.metadata['Language'] = self.target_lang
            
        # Add translator info
        po.metadata['X-Translated-By'] = 'AsyncTranslator (Google Translate API)'
        po.metadata['X-Translation-Date'] = time.strftime('%Y-%m-%d %H:%M%z')
        
        # Don't modify other metadata
        
    async def batch_translate_po_files(self, 
                                      input_files: List[str], 
                                      output_dir: str,
                                      skip_translated: bool = True,
                                      update_fuzzy: bool = True) -> Dict[str, TranslationStats]:
        """
        Translate multiple PO files.
        
        Args:
            input_files: List of input PO file paths
            output_dir: Directory where translated files will be saved
            skip_translated: Skip entries that already have translations
            update_fuzzy: Update entries marked as fuzzy
            
        Returns:
            Dictionary mapping file paths to their TranslationStats
        """
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        all_stats = {}
        
        for input_file in input_files:
            filename = os.path.basename(input_file)
            output_file = os.path.join(output_dir, filename)
            
            self.logger.info(f"Processing file: {input_file} -> {output_file}")
            
            try:
                stats = await self.translate_po_file(
                    input_file=input_file,
                    output_file=output_file,
                    skip_translated=skip_translated,
                    update_fuzzy=update_fuzzy
                )
                all_stats[input_file] = stats
            except Exception as e:
                self.logger.error(f"Failed to translate {input_file}: {str(e)}")
                all_stats[input_file] = TranslationStats(failed_entries=1)
        
        return all_stats
    
    async def translate_django_locale_dir(self, 
                                        locale_dir: str,
                                        target_langs: List[str],
                                        skip_translated: bool = True,
                                        update_fuzzy: bool = True) -> Dict[str, Dict[str, TranslationStats]]:
        """
        Translate a Django locale directory structure.
        
        Args:
            locale_dir: Path to Django locale directory
            target_langs: List of target language codes
            skip_translated: Skip entries that already have translations
            update_fuzzy: Update entries marked as fuzzy
            
        Returns:
            Nested dictionary with statistics for each language and file
        """
        all_stats = {}
        
        for lang in target_langs:
            self.target_lang = lang
            lang_stats = {}
            
            # Find all PO files for this language
            lang_dir = os.path.join(locale_dir, lang, 'LC_MESSAGES')
            
            if not os.path.exists(lang_dir):
                self.logger.info(f"Creating directory structure for language: {lang}")
                os.makedirs(lang_dir, exist_ok=True)
                
                # Copy PO files from source language (usually 'en')
                src_lang_dir = os.path.join(locale_dir, 'en', 'LC_MESSAGES')
                if os.path.exists(src_lang_dir):
                    for po_file in Path(src_lang_dir).glob('*.po'):
                        dest_file = os.path.join(lang_dir, po_file.name)
                        self.logger.info(f"Copying template file: {po_file} -> {dest_file}")
                        # Create a blank copy 
                        po = polib.pofile(str(po_file))
                        # Clear all translations
                        for entry in po:
                            if entry.msgid != '':  # Keep the header
                                entry.msgstr = ''
                        po.save(dest_file)
            
            # Find all PO files in the language directory
            po_files = list(Path(lang_dir).glob('*.po'))
            
            if not po_files:
                self.logger.warning(f"No PO files found for language: {lang}")
                continue
                
            self.logger.info(f"Found {len(po_files)} PO files for language: {lang}")
            
            # Translate each PO file
            for po_file in po_files:
                try:
                    stats = await self.translate_po_file(
                        input_file=str(po_file),
                        output_file=str(po_file),  # Overwrite the original
                        skip_translated=skip_translated,
                        update_fuzzy=update_fuzzy
                    )
                    lang_stats[str(po_file)] = stats
                except Exception as e:
                    self.logger.error(f"Failed to translate {po_file}: {str(e)}")
                    lang_stats[str(po_file)] = TranslationStats(failed_entries=1)
            
            all_stats[lang] = lang_stats
        
        return all_stats

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Translate PO files for Django projects')
    
    # Basic arguments
    parser.add_argument('-i', '--input', required=True, help='Input PO file or directory')
    parser.add_argument('-o', '--output', help='Output file or directory (defaults to overwriting input)')
    parser.add_argument('-s', '--source-lang', default='en', help='Source language code (default: en)')
    parser.add_argument('-t', '--target-lang', required=True, help='Target language code')
    
    # Advanced options
    parser.add_argument('--django-mode', action='store_true', help='Process as Django locale directory')
    parser.add_argument('--batch-size', type=int, default=50, help='Translation batch size')
    parser.add_argument('--concurrency', type=int, default=10, help='Max concurrent requests')
    parser.add_argument('--skip-translated', action='store_true', help='Skip already translated entries')
    parser.add_argument('--update-fuzzy', action='store_true', help='Update fuzzy translations')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('po_translation.log')
        ]
    )
    logger = logging.getLogger('po_translator')
    
    # Initialize translator
    translator = POTranslator(
        source_lang=args.source_lang,
        target_lang=args.target_lang,
        max_concurrent=args.concurrency,
        logger=logger
    )
    
    try:
        # Different modes of operation
        if args.django_mode:
            # Django locale directory mode
            logger.info(f"Processing Django locale directory: {args.input}")
            stats = await translator.translate_django_locale_dir(
                locale_dir=args.input,
                target_langs=[args.target_lang],
                skip_translated=args.skip_translated,
                update_fuzzy=args.update_fuzzy
            )
            
            # Print summary
            logger.info("\nTranslation Summary (Django mode):")
            for lang, lang_stats in stats.items():
                total_translated = sum(s.translated_entries for s in lang_stats.values())
                total_entries = sum(s.total_entries for s in lang_stats.values())
                logger.info(f"Language {lang}: {total_translated}/{total_entries} entries translated")
                
        elif os.path.isdir(args.input):
            # Directory mode
            input_files = list(Path(args.input).glob('**/*.po'))
            output_dir = args.output or args.input
            
            logger.info(f"Found {len(input_files)} PO files in directory: {args.input}")
            stats = await translator.batch_translate_po_files(
                input_files=[str(f) for f in input_files],
                output_dir=output_dir,
                skip_translated=args.skip_translated,
                update_fuzzy=args.update_fuzzy
            )
            
            # Print summary
            logger.info("\nTranslation Summary (Directory mode):")
            total_translated = sum(s.translated_entries for s in stats.values())
            total_entries = sum(s.total_entries for s in stats.values())
            logger.info(f"Total: {total_translated}/{total_entries} entries translated")
            
        else:
            # Single file mode
            output_file = args.output or args.input
            
            logger.info(f"Translating single PO file: {args.input} -> {output_file}")
            stats = await translator.translate_po_file(
                input_file=args.input,
                output_file=output_file,
                skip_translated=args.skip_translated,
                update_fuzzy=args.update_fuzzy,
                batch_size=args.batch_size
            )
            
            # Print summary
            logger.info("\nTranslation Summary (Single file mode):")
            logger.info(f"Translated {stats.translated_entries}/{stats.total_entries} entries")
            logger.info(f"Skipped: {stats.skipped_entries}, Failed: {stats.failed_entries}")
            logger.info(f"Time taken: {stats.time_taken:.2f} seconds")
            
    except Exception as e:
        logger.error(f"Translation process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__": 
    # Set up asyncio policies for Windows if needed
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy') and hasattr(asyncio, 'set_event_loop_policy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
