from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def read_prem(filename):
    """读取PREM模型文件，返回深度、密度、P波速度、S波速度"""
    data = np.loadtxt(filename, skiprows=3)
    radius = data[:, 0]
    depth = 6371 - radius
    rho = data[:, 1]
    vp = data[:, 2]  # Vpv
    vs = data[:, 3]  # Vsv
    return {
        'radius': data[:, 0],      # 半径 (km)
        'rho': data[:, 1],         # 密度 (g/cm³)
        'vpv': data[:, 2],         # 垂直 P 波速度 (km/s)
        'vsv': data[:, 3],         # 垂直 S 波速度 (km/s)
        'qkappa': data[:, 4],      # 体波衰减
        'qshear': data[:, 5],      # 剪切波衰减
        'vph': data[:, 6],         # 水平 P 波速度
        'vsh': data[:, 7],         # 水平 S 波速度
        'eta': data[:, 8]          # 各向异性参数
    }


def plot_model(reference_model_file):
    """绘制密度和速度剖面图"""
    data = read_prem(reference_model_file)
    depth = 6371 - data['radius']  # 半径转深度 (km)

    fig, axes = plt.subplots(1, 2, figsize=(8, 6))

    # 密度图
    axes[0].plot(data['rho'], depth, "k-", linewidth=1.5)
    axes[0].set_xlabel("Density (g/cm³)")
    axes[0].set_ylabel("Depth (km)")
    axes[0].invert_yaxis()
    axes[0].grid(True, alpha=0.3)

    # 速度图
    axes[1].plot(data['vpv'], depth, "b-", label="Vpv", linewidth=1.5)
    axes[1].plot(data['vph'], depth, "b--", label="Vph", linewidth=1.5)
    axes[1].plot(data['vsv'], depth, "r-", label="Vsv", linewidth=1.5)
    axes[1].plot(data['vsh'], depth, "r--", label="Vsh", linewidth=1.5)
    axes[1].set_xlabel("Velocity (km/s)")
    axes[1].set_ylabel("Depth (km)")
    axes[1].invert_yaxis()
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    output_file = Path("images") / Path(reference_model_file).with_suffix(".png").name
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file)


if __name__ == "__main__":
    plot_model("data/mcmc/prem_ocean.txt")
