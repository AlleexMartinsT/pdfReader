import pdfplumber
import tempfile
import logging
import re
import os
import hashlib
import shutil
import versioninfo
import subprocess
import json,difflib
import ctypes
import threading
import tkinter as tk
import queue
import tkinter.messagebox as messagebox
import pandas as pd
import openpyxl
import time
import customtkinter
from oauth2client.service_account import ServiceAccountCredentials
from tkinter import filedialog, ttk, messagebox
from PIL import Image, ImageTk, ImageEnhance
from gerar_versionfile import gerar_versionfile
