import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, HostListener } from '@angular/core';
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

  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private renderer!: THREE.WebGLRenderer;
  private coreMesh!: THREE.Mesh;
  private innerRing!: THREE.Mesh;
  private middleRing!: THREE.Mesh;
  private outerRing!: THREE.Mesh;
  private particleSystem!: THREE.Points;
  private clock = new THREE.Clock();

  private mouse = { x: 0, y: 0, targetX: 0, targetY: 0 };
  private state: string = 'idle';
  private audioIntensity: number = 0.0;
  private animFrameId: number = 0;
  private subs: Subscription[] = [];

  constructor(private audioService: JarvisAudioService) {}

  ngAfterViewInit(): void {
    this.initThree();

    this.subs.push(
      this.audioService.avatarState$.subscribe(st => this.setHologramState(st)),
      this.audioService.audioIntensity$.subscribe(val => this.audioIntensity = val)
    );
  }

  ngOnDestroy(): void {
    cancelAnimationFrame(this.animFrameId);
    this.subs.forEach(s => s.unsubscribe());
    if (this.renderer) this.renderer.dispose();
  }

  @HostListener('window:resize')
  onWindowResize(): void {
    if (!this.canvasRef || !this.renderer || !this.camera) return;
    const canvas = this.canvasRef.nativeElement;
    const width = canvas.clientWidth || 300;
    const height = canvas.clientHeight || 240;
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
    const height = canvas.clientHeight || 250;

    this.scene = new THREE.Scene();
    this.camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 1000);
    this.camera.position.z = 15;

    this.renderer = new THREE.WebGLRenderer({
      canvas: canvas,
      alpha: true,
      antialias: true
    });
    this.renderer.setSize(width, height);
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

    const ambientLight = new THREE.AmbientLight(0xffffff, 0.8);
    this.scene.add(ambientLight);

    const pointLight = new THREE.PointLight(0x00f0ff, 2, 50);
    pointLight.position.set(0, 0, 10);
    this.scene.add(pointLight);

    // Build geometries
    this.buildCore();
    this.buildGimbalRings();
    this.buildParticles();

    this.animate();
  }

  private buildCore(): void {
    const geo = new THREE.IcosahedronGeometry(2.2, 2);
    const mat = new THREE.MeshBasicMaterial({ color: 0x00f0ff, wireframe: true, transparent: true, opacity: 0.85 });
    this.coreMesh = new THREE.Mesh(geo, mat);
    this.scene.add(this.coreMesh);

    const innerGeo = new THREE.SphereGeometry(1.2, 16, 16);
    const innerMat = new THREE.MeshBasicMaterial({ color: 0x0088ff, transparent: true, opacity: 0.6 });
    const innerSphere = new THREE.Mesh(innerGeo, innerMat);
    this.coreMesh.add(innerSphere);
  }

  private buildGimbalRings(): void {
    const r1Geo = new THREE.TorusGeometry(3.2, 0.04, 16, 100);
    const r1Mat = new THREE.MeshBasicMaterial({ color: 0x00f0ff, transparent: true, opacity: 0.7 });
    this.innerRing = new THREE.Mesh(r1Geo, r1Mat);
    this.scene.add(this.innerRing);

    const r2Geo = new THREE.TorusGeometry(4.2, 0.05, 16, 100);
    const r2Mat = new THREE.MeshBasicMaterial({ color: 0x0088ff, transparent: true, opacity: 0.6 });
    this.middleRing = new THREE.Mesh(r2Geo, r2Mat);
    this.scene.add(this.middleRing);

    const r3Geo = new THREE.TorusGeometry(5.2, 0.06, 16, 60);
    const r3Mat = new THREE.MeshBasicMaterial({ color: 0x00ff9d, wireframe: true, transparent: true, opacity: 0.5 });
    this.outerRing = new THREE.Mesh(r3Geo, r3Mat);
    this.scene.add(this.outerRing);
  }

  private buildParticles(): void {
    const count = 200;
    const geo = new THREE.BufferGeometry();
    const pos = new Float32Array(count * 3);

    for (let i = 0; i < count * 3; i += 3) {
      const radius = 6 + Math.random() * 4;
      const theta = Math.random() * Math.PI * 2;
      const phi = Math.acos(Math.random() * 2 - 1);
      pos[i] = radius * Math.sin(phi) * Math.cos(theta);
      pos[i + 1] = radius * Math.sin(phi) * Math.sin(theta);
      pos[i + 2] = radius * Math.cos(phi);
    }
    geo.setAttribute('position', new THREE.BufferAttribute(pos, 3));
    const mat = new THREE.PointsMaterial({ color: 0x00f0ff, size: 0.12, transparent: true, opacity: 0.75 });
    this.particleSystem = new THREE.Points(geo, mat);
    this.scene.add(this.particleSystem);
  }

  public setHologramState(newState: string): void {
    this.state = newState;
    if (!this.coreMesh) return;
    const mat = this.coreMesh.material as THREE.MeshBasicMaterial;
    if (newState === 'thinking') mat.color.setHex(0xffb800);
    else if (newState === 'executing') mat.color.setHex(0x00ff9d);
    else if (newState === 'listening') mat.color.setHex(0xff3366);
    else mat.color.setHex(0x00f0ff);
  }

  private animate = (): void => {
    this.animFrameId = requestAnimationFrame(this.animate);
    const delta = this.clock.getDelta();
    const time = this.clock.getElapsedTime();

    this.mouse.x += (this.mouse.targetX - this.mouse.x) * 0.05;
    this.mouse.y += (this.mouse.targetY - this.mouse.y) * 0.05;

    let speedMult = 1.0;
    if (this.state === 'thinking') speedMult = 3.2;
    if (this.state === 'executing') speedMult = 2.0;
    if (this.state === 'speaking') speedMult = 1.5;

    if (this.coreMesh) {
      this.coreMesh.rotation.x += 0.4 * delta * speedMult;
      this.coreMesh.rotation.y += 0.6 * delta * speedMult;
      const pulse = 1.0 + Math.sin(time * 3) * 0.06 + (this.audioIntensity * 0.35);
      this.coreMesh.scale.set(pulse, pulse, pulse);
    }

    if (this.innerRing) {
      this.innerRing.rotation.x = time * 0.8 * speedMult;
      this.innerRing.rotation.y = time * 0.5 * speedMult;
    }
    if (this.middleRing) {
      this.middleRing.rotation.y = -time * 0.7 * speedMult;
      this.middleRing.rotation.z = time * 0.4 * speedMult;
    }
    if (this.outerRing) {
      this.outerRing.rotation.x = -time * 0.3 * speedMult;
      this.outerRing.rotation.z = -time * 0.6 * speedMult;
    }
    if (this.particleSystem) {
      this.particleSystem.rotation.y = time * 0.15;
      this.particleSystem.rotation.x = time * 0.08;
    }

    this.camera.position.x = this.mouse.x * 2.5;
    this.camera.position.y = this.mouse.y * 2.0;
    this.camera.lookAt(this.scene.position);

    this.renderer.render(this.scene, this.camera);
  };
}
