import pdfplumber

from ml_final_project.config import logger


class PDFParser:

    def __init__(self):
        pass

    def parse(self, pdf_file):
        try:
            try:
                with pdfplumber.open(pdf_file) as pdf:
                    page_container = []

                    for page in pdf.pages:
                        text = page.extract_text()
                        page_container.extend(text.split("\n"))
            except Exception as e:
                logger.error(f"Failed to open file {pdf_file.name}: {e}")
                return None

            jobId = pdf_file.stem
            LocalRegion = self._extract_local_region(page_container)
            PlaceOfAssignment = self._extract_from_list(
                page_container, "Place of Assignment"
            )
            PositionTitle = self._extract_from_list(
                page_container, "Position Title"
            )
            PlantillaNo = self._extract_from_list(
                page_container, "Plantilla Item No."
            )
            SalaryGrade = self._extract_from_list(
                page_container, "Salary/Job/Pay Grade"
            )
            MonthlySalary = self._extract_php(
                self._extract_from_list(page_container, "Monthly Salary")
            )
            Eligibility = self._extract_from_list(page_container, "Eligibility")
            Education = self._extract_from_list(page_container, "Education")
            Training = self._extract_from_list(page_container, "Training")
            Experience = self._extract_from_list(page_container, "Experience")
            Competency = self._extract_from_list(page_container, "Competency")

            return (
                jobId,
                LocalRegion,
                PlaceOfAssignment,
                PositionTitle,
                PlantillaNo,
                SalaryGrade,
                MonthlySalary,
                Eligibility,
                Education,
                Training,
                Experience,
                Competency,
            )
        except Exception as e:
            logger.error(f"Failed to process {pdf_file.name}: {e}")
            return None

    def _extract_from_list(self, page_container: list, key: str) -> str | None:
        try:
            for line in page_container:
                if key in line:
                    # Extract the value after the key
                    value = line.split(key)[1].strip().replace(":", "").strip()
                    return value
        except ValueError:
            return []

    def _extract_local_region(self, page_container: list) -> str | None:
        try:
            return page_container[1].split("|")[0].strip()

        except ValueError:
            return []

    def _extract_php(self, text: str) -> str | None:
        try:
            return text.split("Php")[1].replace(",", "").strip()

        except ValueError:
            return []
