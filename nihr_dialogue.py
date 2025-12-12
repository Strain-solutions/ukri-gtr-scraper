import tkinter as tk
from tkinter import messagebox
from tkinter import ttk  # <--- NEW: Required for the Dropdown menu
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class NIHRConfig:
    search_terms: str
    start_date: str
    end_date: str
    max_rows: int
    mechanism: Optional[str] = None  # None means "All"

def get_scraper_inputs():
    """
    Opens a GUI form to collect search parameters.
    Returns a dictionary of values or None if cancelled.
    """
    # Container for the results
    user_data = {}
    result_container = []

    def submit():
        try:
            # 1. Validation
            terms = entry_terms.get()
            if not terms.strip():
                messagebox.showerror("Error", "Search term cannot be empty")
                return

            # 2. Construct the Dataclass
            config = NIHRConfig(
                search_terms=terms.strip(),
                start_date=entry_start.get(),
                end_date=entry_end.get(),
                max_rows=int(entry_rows.get()),
                mechanism=None if combo_mech.get() == "All" else combo_mech.get()
            )

            # Save to container and close
            result_container.append(config)
            root.destroy()

        except ValueError:
            messagebox.showerror("Error", "Max Rows must be a number.")

    # Setup the window
    root = tk.Tk()
    root.title("NIHR Scraper Config")

    # FORCE ON TOP: This ensures it floats above your IDE
    root.attributes('-topmost', True)

    # Define window size
    window_width = 400
    window_height = 450

    # Get screen dimensions
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    # Calculate center position
    center_x = int(screen_width / 2 - window_width / 2)
    center_y = int(screen_height / 2 - window_height / 2)

    # Set geometry with position: width x height + x_offset + y_offset
    root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')

    # --- UI Layout ---

    # Search Terms
    tk.Label(root, text="Search Terms (separate by comma):").pack(pady=(10, 0))
    entry_terms = tk.Entry(root, width=30)
    entry_terms.insert(0, 'Community AND "Health Intervention"')
    entry_terms.pack(pady=5)

    # --- NEW: Funding Mechanism Dropdown ---
    tk.Label(root, text="Funding Mechanism:").pack()

    # List of common NIHR codes
    mech_options = ["All", "i4i", "PHR", "HTA", "HSDR", "RfPB", "EME", "PGfAR"]

    combo_mech = ttk.Combobox(root, values=mech_options, state="readonly", width=30)
    combo_mech.current(0)  # Select "All" by default
    combo_mech.pack(pady=5)
    # ---------------------------------------

    # Start Date
    tk.Label(root, text="Start Date (YYYY-MM-DD):").pack()
    entry_start = tk.Entry(root, width=20)
    entry_start.insert(0, '2016-01-01')
    entry_start.pack(pady=5)

    # End Date
    tk.Label(root, text="End Date (YYYY-MM-DD):").pack()
    entry_end = tk.Entry(root, width=20)
    entry_end.insert(0, '2025-10-01')
    entry_end.pack(pady=5)

    # Max Rows
    tk.Label(root, text="Max Rows:").pack()
    entry_rows = tk.Entry(root, width=10)
    entry_rows.insert(0, '50')
    entry_rows.pack(pady=5)

    # Submit Button
    tk.Button(root, text="Start Scrape", command=submit, bg="#dddddd").pack(pady=20)

    # Run the window loop
    root.mainloop()

    return  result_container[0] if result_container else None


if __name__ == "__main__":
    user_data = get_scraper_inputs()
    # This will now print something like: {'mechanism': 'i4i', ...} or {'mechanism': None, ...}
    print(user_data)
