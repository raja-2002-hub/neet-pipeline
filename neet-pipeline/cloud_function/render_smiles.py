"""
render_smiles.py

Renders chemical structure diagrams from SMILES notation using RDKit.
Uses rdAbbreviations + explicitMethyl for NEET paper style:
  - CH₃ shown as "CH₃" label (not individual H atoms)
  - CN, OH, CO₂H shown as group labels
  - Benzene rings drawn as hexagons
  - Clean white background, no watermarks
  - Color-coded atoms (O=red, N=blue, Br=brown)

DEPENDENCIES:
  pip install rdkit pillow
"""

import io
from collections import defaultdict

try:
    from rdkit import Chem
    from rdkit.Chem import AllChem, rdAbbreviations
    from rdkit.Chem.Draw import rdMolDraw2D
    RDKIT_AVAILABLE = True
except ImportError:
    RDKIT_AVAILABLE = False
    print("WARNING: rdkit not installed. SMILES rendering disabled.")

from google.cloud import storage

PROJECT         = "project-3639c8e1-b432-4a18-99f"
DIAGRAMS_BUCKET = f"{PROJECT}-diagrams"
IMAGE_SIZE      = (450, 380)

# Load abbreviations once
_ABBREVS = None
def _get_abbrevs():
    global _ABBREVS
    if _ABBREVS is None and RDKIT_AVAILABLE:
        _ABBREVS = rdAbbreviations.GetDefaultAbbreviations()
    return _ABBREVS


def render_smiles_to_png(smiles, size=IMAGE_SIZE):
    """
    Converts SMILES to PNG in NEET paper style.
    
    Key settings:
      rdAbbreviations.CondenseMolAbbreviations → CH₃, CN, OH as labels
      explicitMethyl = True → shows CH₃ at terminal carbons
    """
    if not RDKIT_AVAILABLE or not smiles or not isinstance(smiles, str):
        return None

    smiles = smiles.strip()
    if not smiles:
        return None

    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            print(f"    [SMILES invalid: {smiles}]")
            return None

        AllChem.Compute2DCoords(mol)

        # Apply abbreviations — collapses CH₃, CN, OH, CO₂H into labels
        abbrevs = _get_abbrevs()
        if abbrevs:
            mol = rdAbbreviations.CondenseMolAbbreviations(mol, abbrevs)

        # Render with NEET-style options
        drawer = rdMolDraw2D.MolDraw2DCairo(size[0], size[1])
        opts = drawer.drawOptions()
        opts.explicitMethyl = True    # Show CH₃ at terminal carbons
        opts.bondLineWidth = 2.0      # Clear bond lines
        opts.padding = 0.15           # Clean padding

        drawer.DrawMolecule(mol)
        drawer.FinishDrawing()

        return drawer.GetDrawingText()

    except Exception as e:
        print(f"    [SMILES render error: {smiles} → {e}]")
        return None


def upload_to_gcs(image_bytes, filename, paper_id):
    """Upload PNG bytes to GCS diagrams bucket."""
    storage_client = storage.Client(project=PROJECT)
    blob_path = f"{paper_id}/{filename}"
    blob = storage_client.bucket(DIAGRAMS_BUCKET).blob(blob_path)
    blob.upload_from_string(image_bytes, content_type="image/png")
    return f"gs://{DIAGRAMS_BUCKET}/{blob_path}"


def render_option_diagrams(questions, paper_id):
    """
    Scans all questions for SMILES-based option diagrams.
    Renders each to PNG in NEET style, uploads to GCS.
    """
    if not RDKIT_AVAILABLE:
        print("SMILES rendering skipped — rdkit not available")
        return {}

    print(f"\n{'='*55}")
    print(f"SMILES RENDERING (NEET style) — {paper_id}")
    print(f"{'='*55}")

    url_map = defaultdict(lambda: defaultdict(list))
    rendered_count = 0
    failed_count = 0

    for q in questions:
        section = q.get("section", "")
        q_num = q.get("question_number", 0)
        options = q.get("options", {})

        # Check if any option has SMILES data
        has_smiles = False
        for opt_num in ["1", "2", "3", "4"]:
            opt = options.get(opt_num, "")
            if isinstance(opt, dict) and opt.get("type") == "smiles":
                has_smiles = True
                break

        if not has_smiles:
            continue

        print(f"\n  {section} Q{q_num}:")
        key = f"{section}_{q_num}"

        for opt_num in ["1", "2", "3", "4"]:
            opt = options.get(opt_num, "")
            smiles = None

            if isinstance(opt, dict):
                smiles = opt.get("smiles", None)
            elif isinstance(opt, str) and opt.startswith("SMILES:"):
                smiles = opt[7:].strip()

            if not smiles:
                continue

            png_bytes = render_smiles_to_png(smiles)
            if png_bytes:
                filename = f"smiles_{section.lower()}_q{q_num}_opt{opt_num}.png"
                try:
                    gcs_url = upload_to_gcs(png_bytes, filename, paper_id)
                    url_map[key][f"option_{opt_num}"].append(gcs_url)
                    rendered_count += 1
                    print(f"    opt{opt_num}: {smiles} → ✅")
                except Exception as e:
                    print(f"    opt{opt_num}: upload failed — {e}")
                    failed_count += 1
            else:
                failed_count += 1
                print(f"    opt{opt_num}: {smiles} → ❌")

    print(f"\nSMILES rendering complete:")
    print(f"  Rendered: {rendered_count}")
    print(f"  Failed:   {failed_count}")
    print(f"  Questions: {len(url_map)}")

    return dict(url_map)


def merge_url_maps(main_map, smiles_map):
    """Merges SMILES URLs into main url_map. SMILES takes priority."""
    for key, zones in smiles_map.items():
        for zone, urls in zones.items():
            if urls:
                main_map.setdefault(key, {})[zone] = urls
    return main_map


if __name__ == "__main__":
    test = [
        ("ketene",      "C=C=O"),
        ("tert-nitro",  "CC(C)(C)[N+](=O)[O-]"),
        ("biphenyl",    "c1ccc(-c2ccccc2)cc1"),
        ("ethanol",     "CCO"),
        ("acetic acid", "CC(O)=O"),
        ("aniline",     "Nc1ccccc1"),
        ("nylon-6,6",   "O=C(NCCCCCCNC(=O)CCCCC(=O)O)CCCCC(=O)NCCCCCCN"),
    ]
    print("Testing NEET-style SMILES rendering:\n")
    for name, smi in test:
        png = render_smiles_to_png(smi)
        if png:
            print(f"  ✅ {name:15s} {smi:50s} → {len(png):>6} bytes")
        else:
            print(f"  ❌ {name:15s} {smi}")