import aiohttp
import asyncio
import urllib.parse
import json
import time
import logging
import random
from typing import List, Dict, Optional, Union, Any
from dataclasses import dataclass

# Setting up logging with both file and console handlers
def setup_logging(log_file='translation.log', log_level=logging.INFO):
    """Configure logging to output to both file and console."""
    logger = logging.getLogger('translator')
    logger.setLevel(log_level)
    
    # Clear existing handlers if any
    if logger.handlers:
        logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(console_handler)
    
    return logger

@dataclass
class TranslationResult:
    """Data class to store translation results with metadata."""
    original_text: str
    translated_text: Optional[str]
    source_language: str
    target_language: str
    detected_language: Optional[str] = None
    success: bool = True
    error_message: Optional[str] = None
    time_taken: float = 0.0

class AsyncTranslator:
    """
    Asynchronous translator using Google Translate's API with advanced features:
    - Concurrent batch processing
    - Rate limiting
    - Automatic retries with exponential backoff
    - Comprehensive logging
    - Language detection
    - Translation caching
    """
    
    def __init__(self, 
                 host: str = "https://translate.googleapis.com",
                 max_concurrent_requests: int = 10,
                 request_delay: float = 0.5,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize the translator.
        
        Args:
            host: Translation API host
            max_concurrent_requests: Maximum number of concurrent requests
            request_delay: Delay between requests to avoid rate limiting
            logger: Custom logger (if None, a default one will be created)
        """
        self.host = host
        self.max_concurrent_requests = max_concurrent_requests
        self.request_delay = request_delay
        self.logger = logger or setup_logging()
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.cache = {}  # Simple translation cache
        self.languages = {}  # Language code mappings
        self.total_requests = 0
        self.successful_requests = 0
        self.start_time = None

    async def translate(self, 
                        session: aiohttp.ClientSession, 
                        text: str, 
                        src_lang: str = 'auto', 
                        dest_lang: str = 'en', 
                        retries: int = 3) -> TranslationResult:
        """
        Translates a single text with retries and error handling.
        
        Args:
            session: aiohttp client session
            text: Text to translate
            src_lang: Source language code
            dest_lang: Destination language code
            retries: Number of retry attempts
            
        Returns:
            TranslationResult object containing translation information
        """
        if not text.strip():
            return TranslationResult(
                original_text=text,
                translated_text=text,
                source_language=src_lang,
                target_language=dest_lang,
                success=True
            )
            
        # Check cache first
        cache_key = f"{text}:{src_lang}:{dest_lang}"
        if cache_key in self.cache:
            self.logger.debug(f"Cache hit for: {text[:30]}...")
            return self.cache[cache_key]
        
        # Use semaphore to limit concurrent requests
        async with self.semaphore:
            params = {
                'client': 'gtx',
                'sl': src_lang,
                'tl': dest_lang,
                'dt': ['t', 'ld'],  # 't' for translation, 'ld' for language detection
                'q': text
            }

            url = f"{self.host}/translate_a/single?{urllib.parse.urlencode(params, doseq=True)}"
            
            start_time = time.time()
            detected_lang = None
            self.total_requests += 1
            
            for attempt in range(retries):
                try:
                    # Add delay between requests to avoid rate limiting
                    if attempt > 0:
                        # Exponential backoff
                        backoff_time = (2 ** attempt) + random.uniform(0, 1)
                        self.logger.info(f"Retrying in {backoff_time:.2f} seconds...")
                        await asyncio.sleep(backoff_time)
                    else:
                        # Small delay even on first attempt
                        await asyncio.sleep(self.request_delay * random.uniform(0.8, 1.2))
                    
                    async with session.get(url, timeout=30) as response:
                        if response.status == 429:  # Rate-limit error
                            self.logger.warning(f"Rate-limit error for text: {text[:50]}... - Status: {response.status}")
                            continue
                        elif response.status != 200:
                            raise Exception(f"Translation failed with status: {response.status}")

                        data = await response.json()
                        
                        # Extract translation
                        translated_text = ''.join([sentence[0] for sentence in data[0] if sentence and len(sentence) > 0])
                        
                        # Try to extract detected language if available
                        try:
                            if len(data) > 2 and data[2]:
                                detected_lang = data[2]
                        except (IndexError, TypeError):
                            pass

                        time_taken = time.time() - start_time
                        self.successful_requests += 1
                        
                        # Log successful translation
                        self.logger.info(f"Successfully translated ({time_taken:.2f}s): {text[:50]}... -> {translated_text[:50]}...")
                        
                        result = TranslationResult(
                            original_text=text,
                            translated_text=translated_text,
                            source_language=src_lang,
                            target_language=dest_lang,
                            detected_language=detected_lang,
                            time_taken=time_taken,
                            success=True
                        )
                        
                        # Cache the result
                        self.cache[cache_key] = result
                        return result

                except asyncio.TimeoutError:
                    self.logger.error(f"Timeout on attempt {attempt+1} for text: {text[:50]}...")
                except Exception as e:
                    self.logger.error(f"Attempt {attempt+1} failed for text: {text[:50]}... - Error: {str(e)}")
            
            # If we get here, all retries failed
            time_taken = time.time() - start_time
            self.logger.error(f"Max retries reached for text: {text[:50]}... Skipping.")
            
            return TranslationResult(
                original_text=text,
                translated_text=None,
                source_language=src_lang,
                target_language=dest_lang,
                detected_language=None,
                time_taken=time_taken,
                success=False,
                error_message="Max retries exceeded"
            )

    async def batch_translate(self, 
                             texts: List[str], 
                             src_lang: str = 'auto', 
                             dest_lang: str = 'en') -> List[TranslationResult]:
        """
        Translates a batch of texts concurrently with retries and error handling.
        
        Args:
            texts: List of texts to translate
            src_lang: Source language code
            dest_lang: Destination language code
            
        Returns:
            List of TranslationResult objects
        """
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minutes timeout for the whole session
        
        # Use TCP connector with limit and keepalive
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent_requests,
            enable_cleanup_closed=True,
            keepalive_timeout=60
        )
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [self.translate(session, text, src_lang, dest_lang) for text in texts]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle any exceptions
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error(f"Error processing text at index {i}: {str(result)}")
                    processed_results.append(TranslationResult(
                        original_text=texts[i],
                        translated_text=None,
                        source_language=src_lang,
                        target_language=dest_lang,
                        success=False,
                        error_message=str(result)
                    ))
                else:
                    processed_results.append(result)
                    
            return processed_results

    async def batch_translate_large(self, 
                                   texts: List[str], 
                                   src_lang: str = 'auto', 
                                   dest_lang: str = 'en', 
                                   batch_size: int = 50) -> List[TranslationResult]:
        """
        Handles a very large batch of translations by breaking it into smaller chunks.
        Also provides progress updates.
        
        Args:
            texts: List of texts to translate
            src_lang: Source language code
            dest_lang: Destination language code
            batch_size: Size of each batch
            
        Returns:
            List of TranslationResult objects
        """
        self.start_time = time.time()
        translations = []
        total_batches = (len(texts) + batch_size - 1) // batch_size
        
        self.logger.info(f"Starting translation of {len(texts)} texts from {src_lang} to {dest_lang} in {total_batches} batches")
        
        # Break texts into smaller batches
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            self.logger.info(f"Translating batch {batch_num}/{total_batches} ({len(batch)} texts)")
            
            batch_start_time = time.time()
            batch_translations = await self.batch_translate(batch, src_lang, dest_lang)
            batch_time = time.time() - batch_start_time
            
            translations.extend(batch_translations)
            
            # Calculate and log progress statistics
            success_count = sum(1 for t in batch_translations if t.success)
            elapsed_time = time.time() - self.start_time
            estimated_total = (elapsed_time / batch_num) * total_batches
            remaining_time = estimated_total - elapsed_time
            
            self.logger.info(
                f"Batch {batch_num}/{total_batches} completed in {batch_time:.2f}s: "
                f"{success_count}/{len(batch)} successful. "
                f"Progress: {batch_num/total_batches*100:.1f}%. "
                f"Est. time remaining: {remaining_time/60:.1f} minutes"
            )
            
        # Final statistics
        total_time = time.time() - self.start_time
        success_count = sum(1 for t in translations if t.success)
        
        self.logger.info(
            f"Translation completed in {total_time:.2f}s. "
            f"Success rate: {success_count}/{len(texts)} ({success_count/len(texts)*100:.1f}%). "
            f"Average time per text: {total_time/len(texts):.2f}s"
        )
            
        return translations

    async def translate_file(self, 
                           input_file: str, 
                           output_file: str, 
                           src_lang: str = 'auto', 
                           dest_lang: str = 'en',
                           batch_size: int = 50,
                           delimiter: str = '\n') -> Dict[str, Any]:
        """
        Translates text from a file and saves the results to another file.
        
        Args:
            input_file: Path to input file
            output_file: Path to output file
            src_lang: Source language code
            dest_lang: Destination language code
            batch_size: Size of each batch
            delimiter: Line delimiter
            
        Returns:
            Dictionary with translation statistics
        """
        try:
            # Read input file
            self.logger.info(f"Reading input file: {input_file}")
            with open(input_file, 'r', encoding='utf-8') as f:
                texts = f.read().split(delimiter)
                
            # Filter out empty lines
            texts = [text.strip() for text in texts if text.strip()]
            
            self.logger.info(f"Found {len(texts)} texts to translate")
            
            # Translate
            results = await self.batch_translate_large(texts, src_lang, dest_lang, batch_size)
            
            # Write output file
            self.logger.info(f"Writing translations to: {output_file}")
            with open(output_file, 'w', encoding='utf-8') as f:
                for result in results:
                    if result.success and result.translated_text:
                        f.write(result.translated_text + delimiter)
                    else:
                        f.write(f"[TRANSLATION FAILED: {result.error_message}]{delimiter}")
            
            # Gather statistics
            success_count = sum(1 for r in results if r.success)
            total_time = time.time() - self.start_time if self.start_time else 0
            
            stats = {
                "total_texts": len(texts),
                "successful_translations": success_count,
                "failed_translations": len(texts) - success_count,
                "success_rate": success_count / len(texts) if texts else 0,
                "total_time": total_time,
                "average_time_per_text": total_time / len(texts) if texts else 0
            }
            
            self.logger.info(f"Translation task completed. Success rate: {stats['success_rate']*100:.1f}%")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error translating file: {str(e)}")
            raise

async def translate_text(text: str, source_lang: str = 'auto', target_lang: str = 'en') -> str:
    """Simple helper function for quick translations."""
    translator = AsyncTranslator()
    result = await translator.batch_translate([text], source_lang, target_lang)
    if result and result[0].success:
        return result[0].translated_text
    return None

# Example usage
async def main():
    # Example 1: Simple translation
    translator = AsyncTranslator()
    results = await translator.batch_translate_large(
        texts=["Hello world", "How are you?", "Translation is fun"],
        src_lang="en",
        dest_lang="fr",
        batch_size=10
    )
    
    print("\nTranslation Results:")
    for result in results:
        print(f"'{result.original_text}' -> '{result.translated_text}' "
              f"(Detected: {result.detected_language}, Time: {result.time_taken:.2f}s)")
    
    # Example 2: File translation
    # await translator.translate_file(
    #     input_file="input.txt",
    #     output_file="output.txt",
    #     src_lang="en",
    #     dest_lang="es"
    # )

if __name__ == "__main__":
    # Set up asyncio policies for Windows if needed
    if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy') and hasattr(asyncio, 'set_event_loop_policy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
