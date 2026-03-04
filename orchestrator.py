import luigi
from load_source import load_csv_to_src
from init_dwh import setup_dwh
from elt_pipeline import run_elt

class Task1_LoadSource(luigi.Task):
    """Tugas 1: Memindahkan CSV ke staging database (olist_src)"""
    def output(self):
        # Luigi butuh 'bukti' berupa file kalau tugas ini sudah selesai
        return luigi.LocalTarget('status_task1_done.txt')

    def run(self):
        load_csv_to_src()
        # Membuat file bukti setelah sukses
        with self.output().open('w') as f:
            f.write('Sukses Load ke Source')

class Task2_InitDWH(luigi.Task):
    """Tugas 2: Membangun skema Data Warehouse (olist_dwh)"""
    def requires(self):
        # Tugas 2 baru boleh jalan KALAU Tugas 1 sudah ada buktinya
        return Task1_LoadSource()

    def output(self):
        return luigi.LocalTarget('status_task2_done.txt')

    def run(self):
        setup_dwh()
        with self.output().open('w') as f:
            f.write('Sukses Bangun DWH')

class Task3_RunELT(luigi.Task):
    """Tugas 3: Menjalankan transformasi ELT dan SCD"""
    def requires(self):
        # Tugas 3 butuh Tugas 2 selesai
        return Task2_InitDWH()

    def output(self):
        return luigi.LocalTarget('status_task3_done.txt')

    def run(self):
        run_elt()
        with self.output().open('w') as f:
            f.write('Sukses Pipeline ELT')

if __name__ == '__main__':
    print("🤖 Memulai Robot Luigi...")
    # Kita panggil tugas paling akhir, nanti Luigi akan otomatis merunut ke tugas paling awal
    luigi.build([Task3_RunELT()], local_scheduler=True)