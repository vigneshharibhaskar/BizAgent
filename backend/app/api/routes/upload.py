import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db import models
from app.db.session import get_db
from app.schemas.dataset import DatasetResponse, UploadResponse
from app.services import dataset_loader

router = APIRouter()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _save_upload(file: UploadFile, dataset_id: str) -> Path:
    """
    Persist the uploaded file to UPLOAD_DIR before processing.

    Saving to disk first (rather than reading from the in-memory
    SpooledTempFile) is necessary for large CSVs and for pandas, which
    works best with real file paths.

    Returns the absolute Path of the saved file.
    """
    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    safe_filename = f"{dataset_id}_{file.filename}"
    dest = upload_dir / safe_filename

    with dest.open("wb") as out:
        content = file.file.read()
        out.write(content)

    return dest


# ---------------------------------------------------------------------------
# POST /datasets/upload
# ---------------------------------------------------------------------------


@router.post(
    "/upload",
    response_model=UploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload a CSV file and ingest revenue events",
)
def upload_dataset(
    file: UploadFile = File(..., description="CSV file containing revenue events"),
    name: str = Form(..., description="Human-readable label for this dataset"),
    db: Session = Depends(get_db),
) -> UploadResponse:
    """
    Accept a CSV upload, validate the file extension, persist it to disk,
    then invoke the dataset loader service to parse and store the events.

    Returns a summary of the created dataset and the number of events loaded.

    Raises
    ------
    400  if the file is not a .csv.
    422  if the CSV is missing required columns or contains invalid event types.
    500  for unexpected processing errors.
    """
    # Validate file type
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .csv files are accepted.",
        )

    # Save file to disk
    dataset_id = str(uuid.uuid4())
    try:
        file_path = _save_upload(file, dataset_id)
    except OSError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save uploaded file: {exc}",
        )

    # Delegate all ETL logic to the service layer
    try:
        result = dataset_loader.load_dataset(
            file_path=file_path,
            name=name,
            db=db,
            dataset_id=dataset_id,
        )
    except ValueError as exc:
        # Schema or domain validation failures from the service layer
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error during dataset processing: {exc}",
        )

    return result


# ---------------------------------------------------------------------------
# GET /datasets/
# ---------------------------------------------------------------------------


@router.get(
    "/",
    response_model=list[DatasetResponse],
    summary="List all uploaded datasets",
)
def list_datasets(db: Session = Depends(get_db)) -> list[DatasetResponse]:
    """
    Return all Dataset records ordered by upload time descending (newest first).
    """
    datasets = (
        db.query(models.Dataset)
        .order_by(models.Dataset.uploaded_at.desc())
        .all()
    )
    return datasets


# ---------------------------------------------------------------------------
# GET /datasets/{dataset_id}
# ---------------------------------------------------------------------------


@router.get(
    "/{dataset_id}",
    response_model=DatasetResponse,
    summary="Get a single dataset by ID",
)
def get_dataset(dataset_id: str, db: Session = Depends(get_db)) -> DatasetResponse:
    """
    Return a single Dataset record. Raises 404 if the dataset_id does not exist.
    """
    dataset = (
        db.query(models.Dataset).filter(models.Dataset.id == dataset_id).first()
    )

    if dataset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset '{dataset_id}' not found.",
        )

    return dataset
