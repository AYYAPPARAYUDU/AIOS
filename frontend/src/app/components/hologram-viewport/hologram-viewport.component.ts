import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, HostListener, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { JarvisAudioService } from '../../core/services/jarvis-audio.service';
import { Subscription } from 'rxjs';
import * as THREE from 'three';

@Component({
  selector: 'app-hologram-viewport',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './hologram-viewport.component.html',
  styleUrls: ['./hologram-viewport.component.scss']
})
export class HologramViewportComponent implements AfterViewInit, OnDestroy {
  @ViewChild('hologramCanvas') canvasRef!: ElementRef<HTMLCanvasElement>;
  @Input() userMood: string = 'focused';

  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private renderer!: THREE.WebGLRenderer;
  private coreGroup = new THREE.Group();
  private chakraRingInner!: THREE.Mesh;
  private chakraRingMiddle!: THREE.Mesh;
  private chakraRingOuter!: THREE.Mesh;
  private chakraBladesGroup = new THREE.Group();
  private particleSystem!: THREE.Points;
  private clock = new THREE.Clock();

  private mouse = { x: 0, y: 0, targetX: 0, targetY: 0 };
  public state: string = 'idle';
  private audioIntensity: number = 0.0;
  private animFrameId: number = 0;
  private subs: Subscription[] = [];

  private resizeObserver?: ResizeObserver;

  constructor(public audioService: JarvisAudioService) {}

  ngAfterViewInit(): void {
    this.initThree();

    if (this.canvasRef && this.canvasRef.nativeElement) {
      this.resizeObserver = new ResizeObserver(() => {
        this.onWindowResize();
      });
      this.resizeObserver.observe(this.canvasRef.nativeElement);
    }

    this.subs.push(
      this.audioService.avatarState$.subscribe(st => this.setHologramState(st)),
      this.audioService.audioIntensity$.subscribe(val => this.audioIntensity = val)
    );
  }

  ngOnDestroy(): void {
    if (this.resizeObserver) {
      this.resizeObserver.disconnect();
    }
    cancelAnimationFrame(this.animFrameId);
    this.subs.forEach(s => s.unsubscribe());
    if (this.renderer) this.renderer.dispose();
  }

  @HostListener('window:resize')
  onWindowResize(): void {
    if (!this.canvasRef || !this.renderer || !this.camera) return;
    const canvas = this.canvasRef.nativeElement;
    const width = canvas.parentElement?.clientWidth || canvas.clientWidth || 300;
    const height = canvas.parentElement?.clientHeight || canvas.clientHeight || 240;
    this.camera.aspect = width / height;
    this.camera.updateProjectionMatrix();
    this.renderer.setSize(width, height);
  }


  @HostListener('window:mousemove', ['$event'])
  onMouseMove(event: MouseEvent): void {
    this.mouse.targetX = (event.clientX / window.innerWidth) * 2 - 1;
    this.mouse.targetY = -(event.clientY / window.innerHeight) * 2 + 1;
  }

  private initThree(): void {
    const canvas = this.canvasRef.nativeElement;
    const width = canvas.clientWidth || 400;
    const height = canvas.clientHeight || 240;

    this.scene = new THREE.Scene();
    this.camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 1000);
    this.camera.position.z = 14;

    this.renderer = new THREE.WebGLRenderer({
      canvas: canvas,
      alpha: true,
      antialias: true
    });
    this.renderer.setSize(width, height);
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

    const ambientLight = new THREE.AmbientLight(0xffffff, 1.0);
    this.scene.add(ambientLight);

    const goldLight = new THREE.PointLight(0xffd700, 3.0, 70);
    goldLight.position.set(0, 2, 8);
    this.scene.add(goldLight);

    const cyanLight = new THREE.PointLight(0x00f0ff, 2.0, 70);
    cyanLight.position.set(-6, -4, 6);
    this.scene.add(cyanLight);

    this.scene.add(this.coreGroup);
    this.buildSudarshanaChakra();
    this.buildCelestialParticles();

    this.animate();
  }

  private buildSudarshanaChakra(): void {
    // 1. Golden Radiant Core
    const coreGeo = new THREE.IcosahedronGeometry(1.6, 2);
    const coreMat = new THREE.MeshStandardMaterial({
      color: 0xffd700,
      emissive: 0x996500,
      wireframe: true,
      roughness: 0.2,
      metalness: 0.8
    });
    const coreMesh = new THREE.Mesh(coreGeo, coreMat);
    this.coreGroup.add(coreMesh);

    // Inner Glowing Jewel
    const jewelGeo = new THREE.OctahedronGeometry(0.9, 0);
    const jewelMat = new THREE.MeshBasicMaterial({
      color: 0x00f0ff,
      transparent: true,
      opacity: 0.85
    });
    const jewelMesh = new THREE.Mesh(jewelGeo, jewelMat);
    this.coreGroup.add(jewelMesh);

    // 2. Concentric Sudarshana Rings
    const r1Geo = new THREE.TorusGeometry(2.8, 0.05, 16, 100);
    const r1Mat = new THREE.MeshStandardMaterial({ color: 0xffd700, roughness: 0.1, metalness: 0.9 });
    this.chakraRingInner = new THREE.Mesh(r1Geo, r1Mat);
    this.coreGroup.add(this.chakraRingInner);

    const r2Geo = new THREE.TorusGeometry(3.8, 0.04, 16, 100);
    const r2Mat = new THREE.MeshStandardMaterial({ color: 0x00f0ff, roughness: 0.1, metalness: 0.9 });
    this.chakraRingMiddle = new THREE.Mesh(r2Geo, r2Mat);
    this.coreGroup.add(this.chakraRingMiddle);

    const r3Geo = new THREE.TorusGeometry(4.8, 0.06, 16, 80);
    const r3Mat = new THREE.MeshBasicMaterial({ color: 0xffaa00, wireframe: true, transparent: true, opacity: 0.6 });
    this.chakraRingOuter = new THREE.Mesh(r3Geo, r3Mat);
    this.coreGroup.add(this.chakraRingOuter);

    // 3. Sudarshana Chakra Radiating Blades (12 sacred blades)
    const bladeCount = 12;
    for (let i = 0; i < bladeCount; i++) {
      const angle = (i / bladeCount) * Math.PI * 2;
      const bladeGeo = new THREE.ConeGeometry(0.18, 1.2, 4);
      const bladeMat = new THREE.MeshStandardMaterial({ color: 0xffd700, metalness: 0.9, roughness: 0.2 });
      const blade = new THREE.Mesh(bladeGeo, bladeMat);
      blade.position.set(Math.cos(angle) * 3.8, Math.sin(angle) * 3.8, 0);
      blade.rotation.z = angle - Math.PI / 2;
      this.chakraBladesGroup.add(blade);
    }
    this.coreGroup.add(this.chakraBladesGroup);
  }

  private buildCelestialParticles(): void {
    const count = 360;
    const geo = new THREE.BufferGeometry();
    const pos = new Float32Array(count * 3);
    const colors = new Float32Array(count * 3);

    const goldColor = new THREE.Color(0xffd700);
    const cyanColor = new THREE.Color(0x00f0ff);
    const saffColor = new THREE.Color(0xff5500);

    for (let i = 0; i < count * 3; i += 3) {
      const radius = 5 + Math.random() * 5.5;
      const theta = Math.random() * Math.PI * 2;
      const phi = Math.acos(Math.random() * 2 - 1);
      pos[i] = radius * Math.sin(phi) * Math.cos(theta);
      pos[i + 1] = radius * Math.sin(phi) * Math.sin(theta);
      pos[i + 2] = radius * Math.cos(phi);

      const rChoice = Math.random();
      const chosenColor = rChoice < 0.5 ? goldColor : (rChoice < 0.8 ? cyanColor : saffColor);
      colors[i] = chosenColor.r;
      colors[i + 1] = chosenColor.g;
      colors[i + 2] = chosenColor.b;
    }

    geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));

    const mat = new THREE.PointsMaterial({
      size: 0.14,
      vertexColors: true,
      transparent: true,
      opacity: 0.85
    });
    this.particleSystem = new THREE.Points(geo, mat);
    this.scene.add(this.particleSystem);
  }

  public setHologramState(newState: string): void {
    this.state = newState;
  }

  private animate = (): void => {
    this.animFrameId = requestAnimationFrame(this.animate);
    const delta = this.clock.getDelta();
    const time = this.clock.getElapsedTime();

    this.mouse.x += (this.mouse.targetX - this.mouse.x) * 0.05;
    this.mouse.y += (this.mouse.targetY - this.mouse.y) * 0.05;

    let speedMult = 1.0;
    if (this.state === 'thinking') speedMult = 3.5;
    if (this.state === 'executing') speedMult = 2.4;
    if (this.state === 'speaking') speedMult = 1.8;

    // Spin core chakra
    if (this.chakraBladesGroup) {
      this.chakraBladesGroup.rotation.z += 0.8 * delta * speedMult;
    }
    if (this.chakraRingInner) {
      this.chakraRingInner.rotation.x = time * 0.6 * speedMult;
      this.chakraRingInner.rotation.y = time * 0.4 * speedMult;
    }
    if (this.chakraRingMiddle) {
      this.chakraRingMiddle.rotation.y = -time * 0.5 * speedMult;
      this.chakraRingMiddle.rotation.z = time * 0.3 * speedMult;
    }
    if (this.chakraRingOuter) {
      this.chakraRingOuter.rotation.x = -time * 0.3 * speedMult;
      this.chakraRingOuter.rotation.z = -time * 0.5 * speedMult;
    }

    // Audio reactive pulse
    const pulse = 1.0 + Math.sin(time * 3) * 0.05 + (this.audioIntensity * 0.4);
    this.coreGroup.scale.set(pulse, pulse, pulse);

    if (this.particleSystem) {
      this.particleSystem.rotation.y = time * 0.12;
      this.particleSystem.rotation.x = time * 0.06;
    }

    this.camera.position.x = this.mouse.x * 2.2;
    this.camera.position.y = this.mouse.y * 1.8;
    this.camera.lookAt(this.scene.position);

    this.renderer.render(this.scene, this.camera);
  };
}
