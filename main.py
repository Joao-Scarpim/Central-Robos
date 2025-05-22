from gui.app import App
import os
import winshell
from win32com.client import Dispatch

def criar_atalho_logs_na_area_de_trabalho():
    # Caminho absoluto da pasta logs
    pasta_logs = os.path.abspath("logs")

    # Caminho da área de trabalho do usuário
    desktop = winshell.desktop()

    # Caminho do atalho que será criado
    caminho_atalho = os.path.join(desktop, "Logs Robôs.lnk")

    # Se o atalho já existe, não cria novamente
    if not os.path.exists(caminho_atalho):
        shell = Dispatch('WScript.Shell')
        atalho = shell.CreateShortCut(caminho_atalho)
        atalho.Targetpath = pasta_logs
        atalho.WorkingDirectory = pasta_logs
        atalho.IconLocation = os.path.abspath("main.py")  # opcional, pode trocar por um ícone .ico
        atalho.save()

if __name__ == "__main__":
    criar_atalho_logs_na_area_de_trabalho()
    app = App()
    app.mainloop()