declare namespace google {
  namespace maps {
    class Map {
      constructor(el: HTMLElement, opts?: any);
      getBounds(): LatLngBounds | null | undefined;
      addListener(event: string, fn: () => void): void;
      panTo(latLng: any): void;
      setZoom(zoom: number): void;
      data: Data;
    }
    class Marker {
      constructor(opts?: any);
      setMap(map: Map | null): void;
      addListener(event: string, fn: () => void): void;
    }
    class InfoWindow {
      constructor(opts?: any);
      open(map?: Map, marker?: Marker): void;
      close(): void;
      setContent(c: string): void;
      addListener(event: string, fn: () => void): void;
    }
    class LatLngBounds {
      constructor(sw?: any, ne?: any);
      extend(ll: any): void;
      isEmpty(): boolean;
      contains(ll: any): boolean;
    }
    class Point {
      constructor(x: number, y: number);
    }
    class Data {
      addGeoJson(gj: any): void;
      setStyle(style: any): void;
    }
    enum SymbolPath { CIRCLE = 0 }
    namespace event {
      function clearInstanceListeners(obj: any): void;
      function addListenerOnce(instance: any, event: string, fn: () => void): void;
    }
    namespace places {
      class Autocomplete {
        constructor(input: HTMLInputElement, opts?: any);
        addListener(event: string, fn: () => void): void;
        getPlace(): { geometry?: { location?: { lat(): number; lng(): number } }; formatted_address?: string; name?: string };
        setBounds(bounds: any): void;
      }
    }
  }
}
