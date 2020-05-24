from typing import Generic, TYPE_CHECKING

if TYPE_CHECKING:
    GenericMeta = type  # to avoid dynamic base class
else:
    GenericMeta = type(Generic)
