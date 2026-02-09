# File: utils/image_utils.py
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance
import os
import traceback
import platform
import logging

logger = logging.getLogger(__name__)

# System-appropriate font selection
def get_system_font():
    system = platform.system()
    if system == "Windows":
        # Common Windows fonts
        font_paths = [
            "C:/Windows/Fonts/Arial.ttf",
            "C:/Windows/Fonts/consola.ttf",
            "C:/Windows/Fonts/segoeui.ttf"
        ]
    elif system == "Darwin":  # macOS
        font_paths = [
            "/System/Library/Fonts/Helvetica.ttc",
            "/System/Library/Fonts/Monaco.ttf",
            "/Library/Fonts/Arial.ttf"
        ]
    else:  # Linux and others
        font_paths = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/TTF/Arial.ttf",
            "/usr/share/fonts/truetype/freefont/FreeSans.ttf"
        ]
    
    # Try each font until one works
    for path in font_paths:
        if os.path.exists(path):
            return path
    
    # If no system font is found, use PIL's default font
    logger.warning("No system font found, using default font")
    return None

# Initialize fonts once
font_path = get_system_font()
try:
    title_font_size = 16
    content_font_size = 14
    info_font_size = 12
    
    if (font_path):
        title_font = ImageFont.truetype(font_path, title_font_size)
        content_font = ImageFont.truetype(font_path, content_font_size)
        info_font = ImageFont.truetype(font_path, info_font_size)
    else:
        # Use PIL's default font as fallback
        title_font = ImageFont.load_default()
        content_font = ImageFont.load_default()
        info_font = ImageFont.load_default()
        
except Exception as e:
    logger.error(f"Error loading fonts: {e}")
    # Fallback to default font if there's an error
    title_font = ImageFont.load_default()
    content_font = ImageFont.load_default()
    info_font = ImageFont.load_default()

def generate_card_image(results: list, total_results: int, page: int, total_pages: int) -> Image:
    try:
        # Limit the number of results to process for better performance
        max_results_to_process = min(len(results), 10)
        results = results[:max_results_to_process]
        
        padding = 20
        border_width = 2
        text_color = (255, 255, 255)  # Pure white for all text
        highlight_color = (255, 255, 255)  # Keep highlight color white
        background_color = (0, 0, 0, 255)  # Fully opaque black background
        line_spacing = 4
        result_spacing = 15
        
        # Determine the number of columns based on the number of results
        num_columns = min(3, max(1, (len(results) + 1) // 2))
        max_width = 400 * num_columns  # Adjust width based on number of columns
        column_width = (max_width - padding * (num_columns + 1)) // num_columns
        columns = [[] for _ in range(num_columns)]
        column_heights = [0] * num_columns
        
        # Process results in batches for efficiency
        for i, result in enumerate(results):
            formatted_result = []
            result_height = 0
            lines = result.split('\n')
            for j, line in enumerate(lines):
                font = title_font if j == 0 else content_font
                try:
                    # Handle potential encoding issues safely
                    safe_text = line.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                    wrapped_lines = wrap_text(safe_text, font, column_width)
                    formatted_result.extend(wrapped_lines)
                    
                    # Calculate line heights safely
                    for l in wrapped_lines:
                        try:
                            # PIL 9.0+ compatible way of getting text dimensions
                            bbox = font.getbbox(l)
                            line_height = bbox[3] - bbox[1] if bbox else font.size
                            result_height += line_height + line_spacing
                        except Exception:
                            # Fallback for older PIL versions or errors
                            result_height += font.size + line_spacing
                except Exception as e:
                    logger.warning(f"Error processing text line: {e}")
                    formatted_result.append("[Error rendering text]")
                    result_height += font.size + line_spacing
            
            result_height += result_spacing
            
            # Add to the current column
            current_column = i % num_columns
            columns[current_column].append(formatted_result)
            column_heights[current_column] += result_height
        
        content_height = max(column_heights) if column_heights else padding * 2
        image_height = max(100, content_height + padding * 2 + border_width * 2)  # Ensure minimum height
        image_width = max_width
        
        # Create the base image with black background
        base_image = Image.new('RGBA', (image_width, image_height), background_color)
        draw = ImageDraw.Draw(base_image)
        
        # Draw thin white border
        draw.rectangle([0, 0, image_width-1, image_height-1], outline=(255, 255, 255, 255), width=border_width)
        
        # Create a separate image for the text
        text_image = Image.new('RGBA', (image_width, image_height), (0, 0, 0, 0))
        text_draw = ImageDraw.Draw(text_image)
        
        # Draw the text on the text image
        for col, column_results in enumerate(columns):
            y_offset = padding + border_width
            x_offset = padding + border_width + col * (column_width + padding)
            for result in column_results:
                for i, line in enumerate(result):
                    font = title_font if i == 0 else content_font
                    try:
                        # Handle potential unicode issues
                        text_draw.text((x_offset, y_offset), line, fill=text_color, font=font)
                        try:
                            bbox = font.getbbox(line)
                            line_height = bbox[3] - bbox[1] if bbox else font.size
                            y_offset += line_height + line_spacing
                        except Exception:
                            y_offset += font.size + line_spacing
                    except Exception as e:
                        logger.warning(f"Failed to draw text: {e}")
                        y_offset += font.size + line_spacing
                
                y_offset += result_spacing
        
        # Draw result count information
        showing_results = len(results)
        info_text = f"Showing {showing_results} out of {total_results} results - Page {page}/{total_pages}"
        
        try:
            bbox = info_font.getbbox(info_text)
            info_width = bbox[2] if bbox else len(info_text) * (info_font_size//2)
            info_height = bbox[3] if bbox else info_font_size
            text_draw.text(
                (image_width - padding - info_width, image_height - padding - info_height),
                info_text, fill=highlight_color, font=info_font
            )
        except Exception as e:
            logger.warning(f"Failed to draw info text: {e}")
        
        try:
            # Apply a subtle glow effect to the text
            glow = text_image.filter(ImageFilter.GaussianBlur(radius=0.5))
            glow = ImageEnhance.Brightness(glow).enhance(1.1)
            
            # Composite the images
            image = Image.alpha_composite(base_image, glow)
            image = Image.alpha_composite(image, text_image)
            
            return image
        except Exception as e:
            logger.error(f"Error in image compositing: {e}")
            return base_image  # Return at least the base image if compositing fails
            
    except Exception as e:
        logger.error(f"Error in generate_card_image: {str(e)}")
        traceback.print_exc()  # Print the full traceback
        return None  # Return None if image generation fails

def wrap_text(text: str, font: ImageFont, max_width: int) -> list:
    """Wrap text to fit within a specified width."""
    lines = []
    
    # Handle empty text
    if not text:
        return lines
        
    # Split by words
    words = text.split()
    if not words:
        return lines
        
    # Build lines
    current_line = ""
    for word in words:
        try:
            test_line = current_line + word + " " if current_line else word + " "
            # Check if adding this word would exceed max width
            try:
                text_width = font.getbbox(test_line)[2]
            except:
                # Fallback for older PIL versions
                text_width = font.getlength(test_line) if hasattr(font, 'getlength') else len(test_line) * 7
                
            if text_width <= max_width:
                current_line = test_line
            else:
                if current_line:
                    lines.append(current_line.strip())
                current_line = word + " "
        except Exception:
            # If there's an error processing this word, add it to a new line
            if current_line:
                lines.append(current_line.strip())
            current_line = ""
    
    # Add the last line if it's not empty
    if current_line:
        lines.append(current_line.strip())
        
    return lines