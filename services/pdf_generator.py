import os
from reportlab.lib.pagesizes import letter, A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.colors import Color
from typing import Dict, Any, List

class PDFGenerator:
    def __init__(self, region: str = "US", output_path: str = "output.pdf"):
        self.region = region
        self.output_path = output_path
        self.pagesize = letter if region == "US" else A4
        self.width, self.height = self.pagesize
        self.c = canvas.Canvas(self.output_path, pagesize=self.pagesize)
        
        # Consistent with TS spacing
        self.margin = 45
        self.top_margin = 72
        self.line_height = 14
        self.y = self.height - self.top_margin
        
        # Note: TrueType fonts require downloading the .ttf files. 
        # Using standard built-in fonts as equivalents to ensure execution 
        # without external binary font dependencies in the workspace right away.
        self.font = "Helvetica"
        self.bold_font = "Helvetica-Bold"
        self.italic_font = "Helvetica-Oblique"
        self.black_font = "Helvetica-Bold" # Approximation for Arial Black

    def _check_new_page(self, required_height: float = None):
        if required_height is None:
            required_height = self.line_height
            
        if self.y < self.margin + required_height:
            self.c.showPage()
            self.y = self.height - self.top_margin

    def _draw_text(self, text: str, font: str, size: int, color=(0,0,0), x: float = None, y: float = None):
        if x is None:
            x = self.margin
        if y is None:
            y = self.y
            
        self.c.setFont(font, size)
        self.c.setFillColorRGB(*color)
        self.c.drawString(x, y, text)

    def _wrap_text(self, text: str, font: str, size: int, max_width: float) -> List[str]:
        words = text.split()
        lines = []
        current_line = []
        
        for word in words:
            test_line = " ".join(current_line + [word])
            width = self.c.stringWidth(test_line, font, size)
            if width > max_width and current_line:
                lines.append(" ".join(current_line))
                current_line = [word]
            else:
                current_line.append(word)
                
        if current_line:
            lines.append(" ".join(current_line))
        return lines

    def generate_memo(self, data: Dict[str, Any]):
        """
        Mock implementation matching exact typography styles of the reference implementation
        but tailored for the Vendor Spend Manager memo concept.
        """
        # Header (Arial Black equivalent)
        self._draw_text("Vendor Spend Analysis Memo", self.black_font, 24)
        self.y -= self.line_height * 2
        
        # Meta info
        self._draw_text(f"Generated for: {data.get('company', 'Executive Team')}", self.font, 11)
        self.y -= self.line_height
        self._draw_text(f"Total Reviewed Spend: {data.get('total_spend', '$0.00')}", self.font, 11)
        self.y -= self.line_height * 2
        
        # Executive Summary Section
        self._check_new_page()
        self._draw_text("Executive Summary", self.bold_font, 19)
        self.y -= self.line_height * 1.5
        
        summary_text = data.get('executive_summary', 'No summary provided.')
        summary_lines = self._wrap_text(summary_text, self.font, 11, self.width - (self.margin * 2))
        
        for line in summary_lines:
            self._check_new_page()
            self._draw_text(line, self.font, 11)
            self.y -= self.line_height
            
        self.y -= self.line_height
        
        # Recommendations Section (mimics employment history loops in reference)
        self._check_new_page()
        self._draw_text("Recommendations", self.bold_font, 14)
        self.y -= self.line_height * 1.5
        
        for rec in data.get('recommendations', []):
            self._check_new_page(self.line_height * 3)
            
            # Action (Bold)
            self._draw_text(f"{rec['action']} - {rec['target']}", self.bold_font, 11)
            self.y -= self.line_height
            
            # Sub-text (Light gray)
            self._draw_text(f"Estimated Impact: {rec['impact']}", self.font, 9, color=(0.4, 0.4, 0.4))
            self.y -= self.line_height + 3
            
            # Bullets
            for bullet in rec.get('bullets', []):
                self._check_new_page()
                lines = self._wrap_text(bullet, self.font, 10, self.width - (self.margin * 2.5))
                for i, line in enumerate(lines):
                    if i == 0:
                        self._draw_text("•", self.font, 10, x=self.margin)
                    self._draw_text(line, self.font, 10, x=self.margin + 12)
                    self.y -= self.line_height
                self.y -= 1
            self.y -= self.line_height
            
        self.c.save()
        return self.output_path

