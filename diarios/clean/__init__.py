"""Text cleaning and data transformation utilities for Brazilian legal data."""

from diarios.clean.text import *
from diarios.clean.numbers import *
from diarios.clean.geo import *
from diarios.clean.legal import *

letter = "a-zA-Z' çúáéíóàâêôãõÇÚÁÉÍÓÀÂÊÔÃÕ"
estados = list(get_estado_mapping().values())
